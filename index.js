var modeler = require('modeler')
  , hydration = require('hydration')
  , sublevel = require('level-sublevel')
  , microtime = require('microtime')
  , crypto = require('crypto')

function serialize (buffers) {
  var parts = []
    , idx = 0
  buffers.forEach(function (part) {
    var len = Buffer(4);
    if (typeof part === 'string') part = Buffer(part);
    len.writeUInt32BE(part.length, 0);
    parts.push(len);
    idx += len.length;
    parts.push(part);
    idx += part.length;
  });
  return Buffer.concat(parts);
}

function unserialize (buf) {
  var parts = [];
  var l = buf.length, idx = 0;
  while (idx < l) {
    var dlen = buf.readUInt32BE(idx);
    idx += 4;
    var start = idx;
    var end = start + dlen;
    var part = buf.slice(start, end);
    parts.push(part);
    idx += part.length;
  }
  return parts;
}

module.exports = function (_opts) {
  var api = modeler(_opts);

  if (!api.options.db) throw new Error('must pass a levelup db with options.db');
  if (api.options.password) api.options.crypto = {password: api.options.password};
  if (api.options.crypto) {
    if (!api.options.crypto.algorithm) api.options.crypto.algorithm = 'aes-128-cbc';
    if (!api.options.crypto.saltBytes) api.options.crypto.saltBytes = 16;
    if (!api.options.crypto.keyBytes) api.options.crypto.keyBytes = 16;
    if (!api.options.crypto.keyIterations) api.options.crypto.keyIterations = 1024;
    if (!api.options.crypto.hash) api.options.crypto.hash = 'sha256';
  }
  var objects = sublevel(api.options.db).sublevel(api.options.name);
  var keys = sublevel(api.options.db).sublevel(api.options.name + ':keys');

  function hashId (id) {
    if (api.options.crypto) {
      return crypto.createHash(api.options.crypto.hash)
        .update(String(id))
        .digest('base64');
    }
    else return id;
  }

  // entity to string
  function dehydrate (entity) {
    var data = hydration.dehydrate(entity);
    data = Buffer(JSON.stringify(data));
    if (api.options.crypto) {
      var salt = crypto.randomBytes(api.options.crypto.saltBytes);
      var key = crypto.pbkdf2Sync(api.options.crypto.password, salt, api.options.crypto.keyIterations, api.options.crypto.keyBytes);
      var cipher = crypto.createCipheriv(api.options.crypto.algorithm, key, salt);
      var ctxt = Buffer.concat([cipher.update(data), cipher.final()]);
      var parts = [
        salt,
        ctxt
      ];
      return serialize(parts).toString('base64');
    }
    else return data;
  }

  // string to entity
  function hydrate (data) {
    if (api.options.crypto) {
      var parts = unserialize(Buffer(data, 'base64'));
      var key = crypto.pbkdf2Sync(api.options.crypto.password, parts[0], parseInt(api.options.crypto.keyIterations, 10), parseInt(api.options.crypto.keyBytes, 10));
      var decipher = crypto.createDecipheriv(api.options.crypto.algorithm, key, parts[0]);
      var m = decipher.update(parts[1]);
      data = Buffer.concat([m, decipher.final()]).toString();
    }
    data = JSON.parse(data);
    return hydration.hydrate(data);
  }

  function continuable (offset, limit, reverse, cb) {
    (function next () {
      var chunk = [], counted = 0, ended = false;
      var totalLimit = limit ? offset + limit : undefined;
      objects.createValueStream({reverse: reverse, limit: totalLimit})
        .once('error', cb)
        .on('data', function (data) {
          try {
            var entity = hydrate(data);
          }
          catch (e) {
            ended = true;
            return cb(e);
          }
          counted++;
          if (!ended && counted > offset) chunk.push(entity);
          if (chunk.length === limit) end();
        })
        .on('end', end);

      function end () {
        if (ended) return;
        ended = true;
        offset += chunk.length;
        cb(null, chunk, next);
      }
    })();
  }

  var q = [], blocking = false;

  // due to levelup's async nature, execution order can't be guaranteed. Here's
  // a workaround to ensure that load and save calls respect call order.
  function doNext () {
    if (blocking || !q.length) return;

    function unblock () {
      blocking = false;
      (typeof setImmediate === 'function' ? setImmediate : process.nextTick)(doNext);
    }

    blocking = true;
    var args = q.shift(), op = args[0];

    if (op === 'save') {
      var entity = args[1], cb = args[2];
      var batch = [];
      entity.__idx || (entity.__idx = String(microtime.now()));
      if (entity.rev === 1) {
        batch.push({
          type: 'put',
          key: hashId(entity.id),
          value: entity.__idx,
          prefix: keys
        });
      }
      try {
        var data = dehydrate(entity);
      }
      catch (e) {
        unblock();
        return cb(e);
      }
      batch.push({
        type: 'put',
        key: entity.__idx,
        value: data
      });
      objects.batch(batch, function (err) {
        unblock();
        cb(err);
      });
    }
    else if (op === 'load') {
      var id = hashId(args[1]), cb = args[2];
      keys.get(id, function (err, key) {
        if (err && err.name === 'NotFoundError') err = null;
        if (err) {
          unblock();
          return cb(err);
        }
        if (!key) {
          unblock();
          return cb(null, null);
        }
        objects.get(key, function (err, data) {
          unblock();
          if (err && err.name === 'NotFoundError') err = null;
          if (!data || err) {
            return cb(null, null);
          }
          try {
            var entity = hydrate(data);
          }
          catch (e) {
            return cb(e);
          }
          cb(null, entity);
        });
      });
    }
  }

  api._head = function (offset, limit, cb) {
    continuable(offset, limit, false, cb);
  };
  api._tail = function (offset, limit, cb) {
    continuable(offset, limit, true, cb);
  };
  // this dual key nonsense is necessary because leveldb sorts by key, not insertion order.
  api._save = function (entity, cb) {
    q.push(['save'].concat([].slice.call(arguments)));
    doNext();
  };
  api._load = function (id, cb) {
    q.push(['load'].concat([].slice.call(arguments)));
    doNext();
  };
  api._destroy = function (id, cb) {
    id = hashId(id);
    keys.get(id, function (err, key) {
      if (err && err.name === 'NotFoundError') err = null;
      if (err) return cb(err);
      if (!key) return cb();

      objects.batch([
        {type: 'del', key: id, prefix: keys},
        {type: 'del', key: key}
      ], cb);
    });
  };

  return api;
};

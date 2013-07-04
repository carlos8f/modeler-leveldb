var modeler = require('modeler')
  , hydration = require('hydration')
  , sublevel = require('level-sublevel')
  , microtime = require('microtime')

module.exports = function (_opts) {
  var api = modeler(_opts);

  if (!api.options.db) throw new Error('must pass a levelup db with options.db');
  var objects = sublevel(api.options.db).sublevel(api.options.name);
  var keys = sublevel(api.options.db).sublevel(api.options.name + ':keys');

  var opts = {keyEncoding: 'utf8', valueEncoding: 'json'};

  function continuable (offset, limit, reverse, cb) {
    (function next () {
      var chunk = [], counted = 0, ended = false;
      var totalLimit = limit ? offset + limit : undefined;
      objects.createValueStream({reverse: reverse, limit: totalLimit, keyEncoding: 'utf8', valueEncoding: 'json'})
        .once('error', cb)
        .on('data', function (data) {
          try {
            var entity = hydration.hydrate(data);
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
          key: entity.id,
          value: entity.__idx,
          prefix: keys
        });
      }
      try {
        var data = hydration.dehydrate(entity);
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
      objects.batch(batch, opts, function (err) {
        unblock();
        cb(err);
      });
    }
    else if (op === 'load') {
      var id = args[1], cb = args[2];
      keys.get(id, opts, function (err, key) {
        if (err && err.name === 'NotFoundError') err = null;
        if (err) {
          unblock();
          return cb(err);
        }
        if (!key) {
          unblock();
          return cb(null, null);
        }
        objects.get(key, opts, function (err, data) {
          unblock();
          if (err && err.name === 'NotFoundError') err = null;
          if (!data || err) {
            return cb(null, null);
          }
          try {
            var entity = hydration.hydrate(data);
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
    keys.get(id, opts, function (err, key) {
      if (err && err.name === 'NotFoundError') err = null;
      if (err) return cb(err);
      if (!key) return cb();

      objects.batch([
        {type: 'del', key: id, prefix: keys},
        {type: 'del', key: key}
      ], opts, cb);
    });
  };

  return api;
};

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

  api._head = function (offset, limit, cb) {
    continuable(offset, limit, false, cb);
  };
  api._tail = function (offset, limit, cb) {
    continuable(offset, limit, true, cb);
  };
  // this dual key nonsense is necessary because leveldb sorts by key, not insertion order.
  api._save = function (entity, cb) {
    var batch = [];
    entity.__idx || (entity.__idx = String(microtime.now()));
    var sortableId = entity.__idx + ':' + entity.id;
    if (entity.rev === 1) {
      batch.push({
        type: 'put',
        key: entity.id,
        value: sortableId,
        prefix: keys
      });
    }
    try {
      var data = hydration.dehydrate(entity);
    }
    catch (e) {
      return cb(e);
    }
    batch.push({
      type: 'put',
      key: sortableId,
      value: data
    });
    objects.batch(batch, opts, cb);
  };
  api._load = function (id, cb) {
    keys.get(id, opts, function (err, key) {
      if (err && err.name === 'NotFoundError') err = null;
      if (err) return cb(err);
      if (!key) return cb(null, null);
      objects.get(key, opts, function (err, data) {
        if (err && err.name === 'NotFoundError') err = null;
        if (!data || err) return cb(null, null);
        try {
          var entity = hydration.hydrate(data);
        }
        catch (e) {
          return cb(e);
        }
        cb(null, entity);
      });
    });
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

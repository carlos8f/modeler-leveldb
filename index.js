var modeler = require('modeler')
  , hydration = require('hydration')
  , sublevel = require('level-sublevel')
  , microtime = require('microtime')

module.exports = function (_opts) {
  var api = modeler(_opts);

  if (!api.options.db) throw new Error('must pass a levelup db with options.db');
  var db = sublevel(api.options.db).sublevel(api.options.name);

  // this dual key nonsense is necessary because leveldb sorts by key, not insertion order.

  api._tail = function (limit, cb) {
    var entities = [], ended = false;
    db.createReadStream({reverse: true, keyEncoding: 'utf8', valueEncoding: 'json'})
      .once('error', cb)
      .on('data', function (data) {
        if (!ended && !~data.key.indexOf('_key:')) entities.push(data.value);
        if (entities.length === limit) end();
      })
      .on('end', end);

    function end () {
      if (ended) return;
      ended = true;
      cb(null, entities);
    }
  };
  api._save = function (entity, cb) {
    var batch = [];
    if (entity.rev === 1) {
      entity.__idx = String(microtime.now());
      batch.push({
        type: 'put',
        key: '_key:' + entity.id,
        value: entity.__idx + ':' + entity.id
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
      key: entity.__idx + ':' + entity.id,
      value: data
    });
    db.batch(batch, {keyEncoding: 'utf8', valueEncoding: 'json'}, cb);
  };
  api._load = function (id, cb) {
    db.get('_key:' + id, {keyEncoding: 'utf8', valueEncoding: 'json'}, function (err, key) {
      if (err && err.name === 'NotFoundError') err = null;
      if (err) return cb(err);
      if (!key) return cb(null, null);
      db.get(key, {keyEncoding: 'utf8', valueEncoding: 'json'}, function (err, data) {
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
    db.get('_key:' + id, {keyEncoding: 'utf8', valueEncoding: 'json'}, function (err, key) {
      if (err && err.name === 'NotFoundError') err = null;
      if (err) return cb(err);
      if (!key) return cb();

      db.batch([
        {type: 'del', key: key},
        {type: 'del', key: '_idx:' + id}
      ], {keyEncoding: 'utf8'}, cb);
    });
  };

  return api;
};

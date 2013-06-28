var modeler = require('modeler')
  , hydration = require('hydration')
  , sublevel = require('level-sublevel')

module.exports = function (_opts) {
  var api = modeler(_opts);

  if (!api.options.db) throw new Error('must pass a levelup db with options.db');
  var db = sublevel(api.options.db).sublevel(api.options.name);

  api._list = function (options, cb) {
    var keys = [];
    db.createKeyStream({reverse: options.reverse, keyEncoding: 'utf8'})
      .once('error', cb)
      .on('data', function (key) {
        keys.push(key);
      })
      .on('end', function () {
        cb(null, keys.slice(options.start, options.stop));
      });
  };
  api._save = function (entity, cb) {
    try {
      var data = hydration.dehydrate(entity);
    }
    catch (e) {
      return cb(e);
    }
    db.put(entity.id, data, {keyEncoding: 'utf8', valueEncoding: 'json'}, cb);
  };
  api._load = function (id, cb) {
    db.get(id, {keyEncoding: 'utf8', valueEncoding: 'json'}, function (err, data) {
      if (err && err.name !== 'NotFoundError') return cb(err);
      if (!data || err) return cb(null, null);
      try {
        var entity = hydration.hydrate(data);
      }
      catch (e) {
        return cb(e);
      }
      cb(null, entity);
    });
  };
  api._destroy = function (id, cb) {
    db.del(id, {keyEncoding: 'utf8'}, cb);
  };

  return api;
};

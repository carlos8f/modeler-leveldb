assert = require('assert');
util = require('util');
modeler = require('../');
idgen = require('idgen');
rimraf = require('rimraf');

var dbPath = '/tmp/modeler-leveldb-test-' + idgen();

extraOptions = {
  db: require('level')(dbPath)
};

tearDown = function (done) {
  extraOptions.db.close(function () {
    rimraf(dbPath, done);
  });
};

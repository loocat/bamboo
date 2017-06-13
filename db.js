var log = require('./logger').logger();
var db;

exports.getDB = function(dbms) {
  if (!db && dbms) {
    if (dbms == 'MySQL') {
      db = require('./mysql');
    }
    else if (dbms == 'MongoDB') {
      db = require('./mongo');
    }
    else if (dbms == 'HANA') {
      db = require('./hana');
    }
    else {
      log.error('Unknown database.');
    }
  }
  return db;
}
var filename = process.argv[2] || 'conf.json';
require('fs').readFile(filename, 'utf-8', function (err, data) {
  if (err) {
    console.error("FATAL An error occurred trying to read in the file: " + err);
    //throw new Error(err);
  }
  else {
    launch(JSON.parse(data)['m2m:conf']);
  }
});


var launch = function (conf) {
  var process = require('process');
  var cluster = require('cluster');
  var log = require('./logger').logger(conf.logger);

  if (cluster.isMaster) {
    log.debug('master ' + process.pid);

    var labels = require('querystring').parse(conf.cse.labels) || {};

    var modes = ['PROTOCOL'];
    if (conf.cse.labels && labels.sts === 'true') {
      modes.push('STATUS');
    }
    if (conf.cse.labels && labels.daq === 'true') {
      modes.push('DATA');
    }
    if (conf.cse.labels && labels.mgmt === 'true') {
      modes.push('MGMT');
    }
    
    var workers = {};
    for (var i = 0; i < modes.length; ++i) {
      var mode = modes[i];
      var worker = cluster.fork({ mode: mode });
      workers[worker.process.pid] = mode;
    }

    cluster.on('exit', function (worker, code, signal) {
      log.info('worker %d died (%s). restarting...', worker.process.pid, signal || code);
      var mode = workers[worker.process.pid];
      var newWorker = cluster.fork({ mode: mode });
      workers[newWorker.process.pid] = mode;
      delete workers[worker.process.pid];
    });
  }
  else if (cluster.isWorker) {
    log.info('worker %s running in %s mode', cluster.worker.process.pid, process.env.mode);

    if (!conf) {
      log.error('no config: ' + filename);
    }
    if (!conf.cse) {
      log.error('no CSE config');
    }
    if (!conf.db) {
      log.error('no DB config');
    }

    for (var ii in conf.cse.bind) {
      var bind = conf.cse.bind[ii];
      if (!bind.host || bind.host === 'localhost' || bind.host === '127.0.0.1') {
        bind.host = require('ip').address();
      }
    }
    
    var db = require('./db').getDB(conf.db);
    var service = require('./' + process.env.mode.toLowerCase());
    if (service && db) {
      db.init(conf[conf.db], function (err) {
        if (err) {
          log.error('db connection failed.');
        }
        else {
          service.init(conf.cse);
        }
      });
    }
  }
}

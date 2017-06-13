var winston = require('winston');
var DailyRotateFile = require('winston-daily-rotate-file');
var winstonRemoteTransport = require('winston-remote').Transport;
var moment = require('moment');

var defaultConfig = {
  isUseLogFile: true,
  isUseRemoteLog: false,
  consoleLevel: 'info',
  fileLevel: 'info',
  remoteLevel: 'info',
  isJson: true,
  logLabel: 'crossflow',
  logFileName: function () { return __dirname + '/log/' + this.logLabel; },
  logFileDatePattern: '.yyyy-MM-dd.log',
  logTimestampFormat: 'YYYY-MM-DD HH:mm:ss.SSS',
  remoteHost: 'localhost', // Remote log server ip
  remotePort: 5200 // Remote log server port
};

var logger = undefined;

var consoleTransport = function (config) {
  return new (winston.transports.Console)({
    level: config.consoleLevel,
    timestamp: function () {
      return moment().format(config.logTimestampFormat);
    }
  });
};

var fileTransport = function (config) {
  return new DailyRotateFile({
    level: config.fileLevel,
    json: config.isJson,
    filename: config.logFileName,
    datePattern: config.logFileDatePattern,
    timestamp: function () {
      return moment().format(config.logTimestampFormat);
    }
  });
};

var remoteTransport = function (config) {
  return new (winstonRemoteTransport)({
    level: config.remoteLevel,
    json: config.isJson,
    label: config.logLabel,
    host: config.remoteHost,
    port: config.remotePort,
    timestamp: function () {
      return moment().format(config.logTimestampFormat);
    }
  });
};

function setLogger(config) {
  if (config.isUseRemoteLog && config.isUseLogFile) {
    return {
      transports: [consoleTransport(config), fileTransport(config), remoteTransport(config)]
    }
  }
  else if (!config.isUseRemoteLog && config.isUseLogFile) {
    return {
      transports: [consoleTransport(config), fileTransport(config)]
    }
  }
  else if (config.isUseRemoteLog && !config.isUseLogFile) {
    return {
      transports: [consoleTransport(config), remoteTransport(config)]
    }
  }
  else if (!config.isUseRemoteLog && !config.isUseLogFile) {
    return {
      transports: [consoleTransport(config)]
    }
  }
}

exports.logger = function (givenConfig) {
  if (!logger || givenConfig) {
    var arr = [defaultConfig];
    if (givenConfig) arr.push(givenConfig);
    var con = {};
    arr.forEach(function (e) {
      for (var ii in e) {
        con[ii] = (typeof e[ii] === 'function') ? e[ii]() : e[ii];
      }
    });
    logger = new (winston.Logger)(setLogger(con));
  }
  return logger;
}

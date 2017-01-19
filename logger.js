/**
 * Copyright (c) 2015, SK Corp.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products derived from this software without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @file
 * @copyright 2015, SK Corp.
 */
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

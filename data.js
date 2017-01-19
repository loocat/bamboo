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

var db = require('./db').getDB();
var m2m = require('onem2m');
var binder = require('./binder');
var log = require('./logger').logger();

/**
 * @description
 * Process QMTT data request
 * 
 * @param {topic} MQTT data topic 
 * @param {message} data information
 * @returns
 */
var handleDataRequest = function (message) {

  message.agent.drop(message);

  if (!message.topic || message.topic.length < 1) return;

  var topic_arr = message.topic.split("/");

  // log.debug('topic = ' + topic);
  // log.debug('message = ' + message);

  if (topic_arr[1] == 'M2M' && topic_arr[2] == 'dat' && topic_arr[3] == 'req') {
    var req = message.request;
    if (req.pc && req.pc.cin && req.pc.cin.con) {
      var content;
      if (typeof req.pc.cin.con === 'string') {
        content = m2m.util.parseJson(req.pc.cin.con);
      }
      if (content) {
        storeData(content);
      }
      else {
        log.error('[daq] json string syntax error');
      }
    }
    else {
      log.error('[daq] missing json object: \'req.pc.cin.con\'');
    }
  }
  else {
    log.error('[daq] topic is not supported: ' + topic);
  }
}

function storeData(dat) {	
  // 수집데이터 DB 등록
  var resourceID = m2m.util.createResourceID(4);
  var resourceName = resourceID;
  var lv = new db.Level();
  lv.parentpath = '/' + csename + '/' + dat.cn;
  lv.path = lv.parentpath + '/' + resourceID;
  lv.resourcetype = '4';
  lv.resourceid = resourceID;
  lv.resourcename = resourceName;
  lv.statetag = 0;
  lv.creationtime = dat.ct;
  lv.lastmodifiedtime = dat.ct;
  lv.content = dat;
  lv.labels = 'dat'; // 수집데이터 라벨

  // log.debug(JSON.stringify(lv, null, '  '));
  // log.debug(csename);

  db.create(lv, function (err, res) {
    if (err) {
      log.error("Error storing Data : %s ", err);
    }
  });
}

var csename;
exports.init = function (cse) {
  var mqtt = cse.bind.mqtt;
  if (!mqtt) {
    throw new Error('[daq] require mqtt binding');
  }
  csename = cse.name;
  var broker = 'mqtt://' + mqtt.host + (mqtt.port ? ':' + mqtt.port : '');
  var share = require('querystring').parse(cse.labels).share;
  var agent = new binder.MqttBinder(cse.id, '/M2M/dat', share);
  agent.listen(broker, 'any', handleDataRequest);
}
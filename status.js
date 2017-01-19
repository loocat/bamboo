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
var moment = require('moment');
var binder = require('./binder');
var m2m = require('onem2m');
var log = require('./logger').logger();

var status = [];
var statusKeepAlive;
var statusSave;
const STATUS_DISCONNECTED_CODE = 1;
const STATUS_NOT_RECEIVED_CODE = 4;
const STATUS_NOT_RECEIVED_MESSAGE = 'No status';

var expire = function (sts) {
  if (sts.sc !== STATUS_NOT_RECEIVED_CODE && sts.sc !== STATUS_DISCONNECTED_CODE) {
    // 상태 변경없음 메모리 아이템 갱신
    sts.sc = STATUS_NOT_RECEIVED_CODE;
    sts.sm = STATUS_NOT_RECEIVED_MESSAGE;
    sts.st = moment().format("YYYYMMDDTHHmmss");
    log.debug(sts);

    // 상태 변경없음 DB 등록
    storeStatus(sts);
  }
}

var resetExpirationTimer = function (sts) {
  if (sts.timer) clearTimeout(sts.timer);
  sts.timer = setTimeout(expire, statusKeepAlive * 60 * 1000, sts);
}

var csename;
var broker;
var cseid;
var agent;
var done = false;
exports.init = function (cse) {

  var mqtt = cse.bind.mqtt;
  if (!mqtt) {
    throw new Error('[sts] require mqtt binding');
  }
  csename = cse.name;
  cseid = cse.id;
  broker = 'mqtt://' + mqtt.host + (mqtt.port ? ':' + mqtt.port : '') + '/admin';
  var share = require('querystring').parse(cse.labels).share;
  agent = new binder.MqttBinder(cse.id, '/M2M/sts', share);
  agent.listen(broker, undefined, handleStatusMessage);

  statusKeepAlive = cse.status.keepAlive || 2; // min
  statusSave = cse.status.save || false;
  log.info('cse.status.keepAlive=' + statusKeepAlive);
  log.info('cse.status.save=' + statusSave);

  if (statusSave) {
    // DB로부터 최신 상태 로딩
    db.getStatus(function (err, rows) {
      if (err) {
        log.error("getStatus Error : %s ", err);
      }
      else {
        for (var i in rows) {
          var sts = JSON.parse(rows[i].content);
          sts.sc = +sts.sc;
          status.push(sts);
          resetExpirationTimer(sts);
        }
        done = true;
      }
    });
  }
  else {
    done = true;
  }
}

function setStatus(sts) {
  if (done) {
    var found = false;
    for (var i in status) {
      if (status[i].nm === sts.nm) {
        // Status Changed
        if (status[i].sc !== sts.sc) {
          // 상태 변경 메모리 아이템 갱신
          status[i].nm = sts.nm;
          status[i].sc = sts.sc;
          status[i].sm = sts.sm;
          status[i].st = sts.st;
          // 상태 변경 DB 등록
          storeStatus(sts);
        }
        resetExpirationTimer(status[i]);
        found = true;
      }
    }

    if (!found) {
      // 상태 변경 메모리 아이템 추가
      status.push(sts);
      // 상태 변경 DB 등록
      storeStatus(sts);
      resetExpirationTimer(sts);
    }
  }
}


var forwardToAdmin = (content) => {
  // 상태 변경 메시지 Admin으로 MQTT Pub('/M2M/sts/req/CSEID/admin')
  var format = 'json';
  var message = m2m.util.wrapMessage('contentInstance', { con: content }, format);

  var rqp = {
    op: m2m.code.getOperation('Create'),
    fr: '/' + csename, // TODO fix '/cseid/csename'
    to: 'admin',
    pc: message,
    cty: format
  };

  agent.sendRQP(broker, rqp);
}

function storeStatus(sts) {
  // 상태정보 DB 저장
  var resourceID = m2m.util.createResourceID(4);
  var resourceName = resourceID;
  var lv = new db.Level();
  lv.parentpath = csename + '/' + sts.nm;
  lv.path = lv.parentpath + '/' + resourceID;
  lv.resourcetype = 4;
  lv.resourceid = resourceID;
  lv.resourcename = resourceName;
  lv.statetag = 0;
  lv.creationtime = sts.st;
  lv.lastmodifiedtime = sts.st;
  lv.content = JSON.stringify({ nm: sts.nm, sc: sts.sc, sm: sts.sm, st: sts.st });

  if (statusSave) {
    db.create(lv, function (err, res) {
      if (!err) {
        log.debug('Status inserted.');
      }
    });
  }

  forwardToAdmin(lv.content);
}

/**
 * @description
 * Process QMTT status request
 * 
 * @param {topic} MQTT status topic 
 * @param {message} status information
 * @returns
 */
var handleStatusMessage = function (message) {

  message.agent.drop(message);

  if (!message.topic || message.topic.length < 1) return;

  var topic_arr = message.topic.split("/");

  // log.debug('message = ' + JSON.stringify(message, null, ' '));

  if (topic_arr[1] == 'M2M' && topic_arr[2] == 'sts' && topic_arr[3] == 'req') {
    var rqp = message.rqp;
    if (rqp && rqp.pc) {
      var pc = (typeof rqp.pc === 'string') ? m2m.util.parseJson(rqp.pc) : rqp.pc;
      if (pc.cin && pc.cin.con) {
        var content = rqp.pc.cin.con;
        if (typeof content === 'string') {
          content = m2m.util.parseJson(content);
        }
        if (content) {
          setStatus(content);
        }
        else {
          log.error('[sts] json string syntax error: \'rqp.pc.cin.com\'');
        }
      }
      else {
        log.error('[sts] missing json object: \'rqp.pc.cin.con\'');
      }
    }
    else {
      log.error('[sts] missing json object: \'rqp.pc\'');
    }
  }
  else {
    log.error('[sts] topic is not supported: ' + topic);
  }
}
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
  var lv = {};
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
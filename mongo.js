/**
 * Copyright (c) 2015, SK Corp.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following FCs are met:
 * 1. Redistributions of source code must retain the above copyright notice, this list of FCs and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of FCs and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products derived from this software without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @file
 * @copyright 2015, SK Corp.
 */

exports.dbms = 'MongoDB';
var log = require('./logger').logger();
var MongoClient = require('mongodb').MongoClient;

var dbConfig = {
  host: 'localhost',
  port: 27017,
  user: 'root',
  password: 'myroot',
  database: 'crossflow',
  maxPoolSize: 20
};

var db;
exports.init = function (config, callback) {
  if (!db) {
    if (config) {
      if (config.host) { dbConfig.host = config.host; }
      if (config.port) { dbConfig.port = config.port; }
      if (config.user) { dbConfig.user = config.user; }
      if (config.password) { dbConfig.password = config.password; }
      if (config.database) { dbConfig.database = config.database; }
      if (config.maxPoolSize) { dbConfig.maxPoolSize = config.maxPoolSize; }
    }

    var url = "mongodb://" + dbConfig.user + ":" + dbConfig.password + "@" + dbConfig.host + ":" + dbConfig.port
      + "/" + dbConfig.database + "?maxPoolSize=" + dbConfig.maxPoolSize;

    MongoClient.connect(url, function (err, database) {
      if (!err) {
        db = database;
      }
      callback(err);
    });
  }
}

exports.Level = function Level() {
  this.parentpath;
  this.path;
  this.resourcetype;
  this.resourceid;
  this.resourcename;
  this.parentid;
  this.creationtime;
  this.lastmodifiedtime;
  this.labels;
  this.accesscontrolpolicyids;
  this.expirationtime;
  this.announceto;
  this.announcedattribute;
  this.csetype;
  this.cseid;
  this.supportedresourcetype;
  this.pointofaccess;
  this.nodelink;
  this.csebase;
  this.m2mextid;
  this.triggerrecipientid;
  this.requestreachability;
  this.appname;
  this.appid;
  this.aeid;
  this.ontologyref;
  this.statetag;
  this.creator;
  this.maxnrofinstances;
  this.maxbytesize;
  this.maxinstanceage;
  this.currentnrofinstances;
  this.currentbytesize;
  this.locationid;
  this.contentinfo;
  this.contentsize;
  this.content;
  this.eventnotificationcriteria;
  this.notificationuri;
  this.groupid;
  this.notificationforwardinguri;
  this.batchnotify;
  this.ratelimit;
  this.pendingnotification;
  this.notificationstoragepriority;
  this.latestnotify;
  this.notificationcontenttype;
  this.notificationeventcat;
  this.expirationcounter;
  this.presubscriptionnotify;
  this.subscriberuri;
}

var fields = {
  parentpath: "$parentpath",
  path: "$path",
  resourcetype: "$resourcetype",
  resourceid: "$resourceid",
  resourcename: "$resourcename",
  parentid: "$parentid",
  creationtime: "$creationtime",
  lastmodifiedtime: "$lastmodifiedtime",
  labels: "$labels",
  accesscontrolpolicyids: "$accesscontrolpolicyids",
  expirationtime: "$expirationtime",
  announceto: "$announceto",
  announcedattribute: "$announcedattribute",
  csetype: "$csetype",
  cseid: "$cseid",
  supportedresourcetype: "$supportedresourcetype",
  pointofaccess: "$pointofaccess",
  nodelink: "$nodelink",
  csebase: "$csebase",
  m2mextid: "$m2mextid",
  triggerrecipientid: "$triggerrecipientid",
  requestreachability: "$requestreachability",
  appname: "$appname",
  appid: "$appid",
  aeid: "$aeid",
  ontologyref: "$ontologyref",
  statetag: "$statetag",
  creator: "$creator",
  maxnrofinstances: "$maxnrofinstances",
  maxbytesize: "$maxbytesize",
  maxinstanceage: "$maxinstanceage",
  currentnrofinstances: "$currentnrofinstances",
  currentbytesize: "$currentbytesize",
  locationid: "$locationid",
  contentinfo: "$contentinfo",
  contentsize: "$contentsize",
  content: "$content",
  eventnotificationcriteria: "$eventnotificationcriteria",
  notificationuri: "$notificationuri",
  groupid: "$groupid",
  notificationforwardinguri: "$notificationforwardinguri",
  batchnotify: "$batchnotify",
  ratelimit: "$ratelimit",
  pendingnotification: "$pendingnotification",
  notificationstoragepriority: "$notificationstoragepriority",
  latestnotify: "$latestnotify",
  notificationcontenttype: "$notificationcontenttype",
  notificationeventcat: "$notificationeventcat",
  expirationcounter: "$expirationcounter",
  presubscriptionnotify: "$presubscriptionnotify",
  subscriberuri: "$subscriberuri"
};

exports.create = function (lv, callback) {
  var lc = lv.path.split('/').length;
  if (lc < 1) {
    log.debug('[create] Path is invalid. ' + lv.path);
    return;
  }

  db.collection('resource').insert(lv, function (err, result) {
    if (err) {
      log.debug("Error inserting : %s ", err);
    }
    else {
      log.info('Inserting Completed.', lv.path);
    }
    if (callback) callback(err, result);
  });
};

exports.retrieve = function (path, fc, callback) {
 
  var lc = path.split('/').length;
  if (lc < 1) {
    log.debug('[retrieve] Path is invalid. ' + path);
    return;
  }

  if (!fc || Object.keys(fc).length == 0) { // 필터조건이 없은 경우 리소스ID로 단건 조회
    db.collection('resource').findOne({ "path": path }, function (err, docs) {
      if (err) {
        log.debug("Error retrieving : %s ", err);
      }
      else {
        log.debug('Retrieving Completed.');
      }
      if (callback) callback(err, docs);
    });
  }
  else {
    if ('lim' in fc) {
      var cursor = db.collection('resource').find(makeFC(path, fc));

      cursor.sort({ "creationtime": -1 });
      cursor.limit(fc.lim);

      cursor.toArray(function (err, docs) {
        if (err) {
          log.debug("Error retrieving : %s ", err);
        }
        else {
          log.debug('Retrieving Completed.');
        }
        if (callback) callback(err, docs);
      });
    }
    else {
      var FC = makeFC(path, fc);
      FC.resourcetype = { $ne: 4 };
      var cursor = db.collection('resource').find(FC);

      var rowsWithoutCI = [];
      cursor.toArray(function (err, docs) {
        if (err) {
          log.debug("Error retrieving : %s ", err);
          if (callback) callback(true, err);
        }
        else {
          rowsWithoutCI = docs;

          FC = makeFC(path, fc);
          FC.resourcetype = 4;
          cursor = db.collection('resource').find(FC).sort({ "creationtime": -1 }).limit(1);

          cursor.toArray(function (err, docs) {
            if (err) {
              log.debug("Error retrieving : %s ", err);
              if (callback) callback(true, err);
            }
            else {
              if (docs && docs.length > 0) {
                rowsWithoutCI.push(docs[0]);
              }
              if (callback) callback(false, rowsWithoutCI);
              log.debug('Retrieving Completed.');
            }
          });
        }
      });
    }
  }
}

exports.update = function (lv, fc, callback) {
  fc = null;  // fc not supported.
  
  var lc = lv.path.split('/').length;
  if (lc < 1) {
    log.debug('[update] Path is invalid. ' + lv.path);
    return;
  }

  db.collection('resource').update({ "path": lv.path }, { $set: lv }, function (err, result) {
    if (err) {
      log.debug("Error updating : %s ", err);
    }
    else {
      log.info('Updating Completed.', lv.path);
    }
    if (callback) callback(err, result);
  });
}

exports.delete = function (path, fc, callback) {
  fc = null;  // fc not supported.
  
  var lc = path.split('/').length;
  if (lc < 1) {
    log.debug('[delete] Path is invalid. ' + path);
    return;
  }
  
  db.collection('resource').remove({ "path": new RegExp('^' + path) }, function (err, result) {
    if (err) {
      log.debug("Error deleting : %s ", err);
    }
    else {
      log.info('Deleting Completed.', path);
    }
    if (callback) callback(err, result);
  });
}

function makeFC(path, fc) {
  var FC = { "parentpath": path };
  if (fc) {
    if (fc.crb != null) {
      FC.creationtime = { $lt: fc.crb };
    }
    if (fc.cra != null) {
      FC.creationtime = { $gt: fc.cra };
    }
    if (fc.ms != null) {
      FC.lastmodifiedtime = { $lt: fc.ms };
    }
    if (fc.us != null) {
      FC.lastmodifiedtime = { $gt: fc.us };
    }
    if (fc.sts != null) {
      FC.statetag = { $lt: fc.sts };
    }
    if (fc.stb != null) {
      FC.statetag = { $gt: fc.stb };
    }
    if (fc.exb != null) {
      FC.expirationtime = { $lt: fc.exb };
    }
    if (fc.exa != null) {
      FC.expirationtime = { $gt: fc.exa };
    }
    if (fc.lbl != null) {
      FC.labels = new RegExp(fc.lbl);
    }
    if (fc.ty != null) {
      if (fc.ty instanceof Array) {
        FC.$or = new Array();
        for (var i = 0; i < fc.ty.length; i++) {
          var ty = {};
          ty.resourcetype = fc.ty[i];
          FC.$or.push(ty);
        }
      } else {
        FC.resourcetype = fc.ty;
      }
    }
    if (fc.sza != null) {
      FC.contentsize = { $lt: fc.sza };
    }
    if (fc.szb != null) {
      FC.contentsize = { $gt: fc.szb };
    }
    if (fc.cty != null) {
      FC.contentinfo = new RegExp(fc.cty);
    }
  }
  return FC;
}

exports.getStatus = function (callback) {
  // Only AE (for Thyme)
  var cursor = db.collection('resource').aggregate([
    { $match: { "parentpath": new RegExp('/sts$'), "resourcetype": 4 } },
    { $sort: { "parentpath": 1, "creationtime": -1 } },
    {
      $group: {
        _id: "$parentpath",
        item: { $first: fields }
      }
    }
  ]);

  cursor.toArray(function (err, docs) {
    if (err) {
      log.debug("Error getStatus : %s ", err);
      if (callback) callback(true, err);
    }
    else {
      log.info('getStatus Completed.');
      if (callback) callback(false, docs);
    }
  });
}
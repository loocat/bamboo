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

exports.dbms = 'MySQL';
var m2m = require('onem2m');
var util = require('util');
var mysql = require('mysql');
var log = require('./logger').logger();

var dbConfig = {
  host: 'localhost',
  port: 3306,
  user: 'root',
  password: 'myroot',
  database: 'crossflow',
  connectionLimit: 20
};

var dbPool;
exports.init = function (config, callback) {
  if (!dbPool) {
    if (config) {
      if (config.host) { dbConfig.host = config.host; }
      if (config.port) { dbConfig.port = config.port; }
      if (config.user) { dbConfig.user = config.user; }
      if (config.password) { dbConfig.password = config.password; }
      if (config.database) { dbConfig.database = config.database; }
      if (config.connectionLimit) { dbConfig.connectionLimit = config.connectionLimit; }
    }
    dbPool = mysql.createPool(dbConfig);
    dbPool.getConnection(function (err, conn) {
      if (conn) conn.release();
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

exports.create = function (lv, callback) {
  var lc = lv.path.split('/').length;
  if (lc < 1) {
    log.debug('[create] Path is invalid. ' + lv.path);
    return;
  }
  dbPool.getConnection(function (err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    if (conn) {
      var sql = "INSERT INTO lv (";
      var first = true;
      var params = new Array();
      var value;
      for (var key in lv) {
        value = lv[key];
        if (value && typeof value == 'object') {
          params.push(JSON.stringify(value));
        } else {
          params.push(value);
        }
        if (first) {
          sql += key;
          first = false;
        }
        else {
          sql += ", " + key;
        }
      }
      sql += ") VALUES (";
      first = true;
      for (var i = 0; i < params.length; i++) {
        if (first) {
          sql += "?";
          first = false;
        }
        else {
          sql += ", ?";
        }
      }
      sql += ")";
      log.debug(sql, params);
      conn.query(sql, params, function (err, res) {
        if (err) {
          log.debug("Error inserting : %s ", err);
          if (callback) callback(true, err);
        }
        else {
          log.info('Inserting Completed.', lv.path);
          if (callback) callback(false, res);
        }
        conn.release();
      });
    }
  });
};

exports.retrieve = function (path, fc, callback) {
  var lc = path.split('/').length;
  if (lc < 1) {
    log.debug('[retrieve] Path is invalid. ' + path);
    return;
  }

  var hierarchical = m2m.util.isStructured(path);

  dbPool.getConnection(function (err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    if (conn) {
      var sql;
      if (!fc || Object.keys(fc).length == 0) { // 필터조건이 없은 경우 단건 리소스 조회
        sql = util.format("SELECT * FROM lv WHERE %s = ?", hierarchical ? 'path' : 'resourceid');
        log.debug(sql, path);
        conn.query(sql, path, function (err, rows) {
          if (err) {
            log.debug("Error retrieving : %s ", err);
            if (callback) callback(true, err);
          }
          else {
            log.debug('Retrieving Completed.');
            if (callback) callback(false, rows);
          }
          conn.release();
        });
      }
      else { // 필터조건이 있는 경우 다건 리소스 조회
        if (('lim' in fc) === false && ('ty' in fc) === false) {
          sql = util.format("SELECT * FROM lv WHERE %s = ? AND resourcetype <> 4", hierarchical ? 'parentpath' : 'parentid');
          var result = makeFC(path, fc, sql);

          result.sql = result.sql.replace(' LIMIT', ' ORDER BY path LIMIT');

          log.debug(result.sql, result.params);
          var rowsWithoutCI = [];
          conn.query(result.sql, result.params, function (err, rows) {
            if (err) {
              log.debug("Error retrieving : %s ", err);
              if (callback) callback(true, err);
              conn.release();
            }
            else {
              rowsWithoutCI = rows;
              // conn.release();

              // dbPool.getConnection(function (err, conn) {
              //   if (err) {
              //     log.debug('db connection failed.');
              //   }
              //   if (conn) {
                  sql = util.format("SELECT * FROM lv WHERE %s = ? AND resourcetype = 4", hierarchical ? 'parentpath' : 'parentid');
                  result = makeFC(path, fc, sql);

                  result.sql += " ORDER BY creationtime DESC LIMIT 1";

                  log.debug(result.sql, result.params);
                  conn.query(result.sql, result.params, function (err, rows) {
                    if (err) {
                      log.debug("Error retrieving : %s ", err);
                      if (callback) callback(true, err);
                    }
                    else {
                      log.debug('Retrieving Completed.');
                      if (rows && rows.length > 0) {
                        rowsWithoutCI.push(rows[0]);
                      }
                      if (callback) callback(false, rowsWithoutCI);
                    }
                    conn.release();
                  });
              //   }
              // });
            }
          });
        }
        else {
          sql = util.format("SELECT * FROM lv WHERE %s = ?", hierarchical ? 'parentpath' : 'parentid');
          var result = makeFC(path, fc, sql);

          result.sql = result.sql.replace(' LIMIT', ' ORDER BY path LIMIT');

          log.debug(result.sql, result.params);
          conn.query(result.sql, result.params, function (err, rows) {
            if (err) {
              log.debug("Error retrieving : %s ", err);
              if (callback) callback(true, err);
            }
            else {
              log.debug('Retrieving Completed.');
              if (callback) callback(false, rows);
            }
            conn.release();
          });
        }
      }
    }
  });
}

exports.update = function (lv, fc, callback) {
  fc = null;  // fc not supported.

  var lc = lv.path.split('/').length;
  if (lc < 1) {
    log.debug('[update] Path is invalid. ' + lv.path);
    return;
  }
  dbPool.getConnection(function (err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    if (conn) {
      var sql;
      var first = true;
      var params = new Array();
      var value;
      if (!fc || Object.keys(fc).length == 0) { // 필터조건이 없은 경우 단건 리소스 수정
        sql = "UPDATE lv SET ";
        for (var key in lv) {
          value = lv[key];
          if (key != 'path') {
            if (value && typeof value == 'object') {
              params.push(JSON.stringify(value));
            } else {
              params.push(value);
            }
            if (first) {
              sql += key + " = ?";
              first = false;
            }
            else {
              sql += ", " + key + " = ?";
            }
          }
        }
        sql += " WHERE path = ?";
        params.push(lv.path);
        log.debug(sql, params);
        conn.query(sql, params, function (err, res) {
          if (err) {
            log.debug("Error updating : %s ", err);
            if (callback) callback(true, err);
          }
          else {
            log.info('Updating Completed.', lv.path);
            if (callback) callback(false, res);
          }
          conn.release();
        });
      }
      else { // 필터조건이 있는 경우 다건 리소스 수정
        sql = "UPDATE lv SET ";
        for (var key in lv) {
          value = lv[key];
          if (value && typeof value == 'object') {
            params.push(JSON.stringify(value));
          } else {
            params.push(value);
          }
          if (first) {
            sql += key + " = ?";
            first = false;
          }
          else {
            sql += ", " + key + " = ?";
          }
        }
        sql += " WHERE path = ?";
        var result = makeFC(lv.path, fc, sql);
        result.params = params.concat(result.params);
        log.debug(result.sql, result.params);
        conn.query(result.sql, result.params, function (err, res) {
          if (err) {
            log.debug("Error updating : %s ", err);
            if (callback) callback(true, err);
          }
          else {
            log.info('Updating Completed.', lv.path);
            if (callback) callback(false, res);
          }
          conn.release();
        });
      }
    }
  });
}

exports.delete = function (path, fc, callback) {
  fc = null;  // fc not supported.

  var lc = path.split('/').length;
  if (lc < 1) {
    log.debug('[delete] Path is invalid. ' + path);
    return;
  }
  dbPool.getConnection(function (err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    if (conn) {
      var sql;
      if (!fc || Object.keys(fc).length == 0) { // 필터조건이 없은 경우 단건 리소스 삭제
        sql = "DELETE FROM lv WHERE path = ?";
        conn.query(sql, path, function (err, res) {
          if (err) {
            log.debug("Error deleting : %s ", err);
            if (callback) callback(true, err);
            conn.release();
          }
          else {
            // for (var i = 1; i <= (9 - lc); i++) {
              sql = "DELETE FROM lv WHERE path LIKE ?";
              var params = [path + "/%"];
              log.debug(sql, params);
              conn.query(sql, params, function (err, res) {
                if (err) {
                  log.debug("Error child deleting : %s ", err);
                  if (callback) callback(true, err);
                }
                else {
                  if (res.length > 0) {
                    log.debug('Child deleting Completed.');
                  }
                  log.info('Deleting Completed.', path);
                  if (callback) callback(false, res);
                }
                conn.release();
              });
            // }
          }
        });
      }
      else { // 필터조건이 있는 경우 다건 리소스 삭제
        sql = "DELETE FROM lv WHERE path = ?"
        var result = makeFC(path, fc, sql);
        log.debug(result.sql, result.params);
        conn.query(result.sql, result.params, function (err, res) {
          if (err) {
            log.debug("Error deleting : %s ", err);
            if (callback) callback(true, err);
          }
          else {
            log.info('Deleting Completed.', path);
            if (callback) callback(false, res);
          }
          conn.release();
        });
      }
    }
  });
}

exports.discovery = function (path, fc, rcn, drt, order, callback) {
  var lc = path.split('/').length;
  if (lc < 1) {
    log.debug('[discovery] Path is invalid. ' + path);
    return;
  }
  if ([4, 5, 6, 8].indexOf(rcn) < 0) {  // check result content values for discovery
    log.debug('[discovery] Result content is invalid. ' + rcn);
    return;
  }

  var childResources = (rcn === 4 || rcn === 8) ? true : false;
  var hierarchical = m2m.util.isStructured(path);

  dbPool.getConnection(function (err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    if (conn) {
      // 필터조건에 따른 다건 리소스 검색
      var sql;
      sql = util.format("SELECT %s FROM (SELECT get_lvl_lv(resourceid) AS resourceid, @level AS level FROM (SELECT @start_with:=%s, @resourceid:=@start_with, @level:=0) vars JOIN lv WHERE @resourceid IS NOT NULL) f JOIN lv d ON f.resourceid = d.resourceid",
        childResources ? 'd.*' : ((drt === 2) ? 'd.resourceid' : 'd.path'), hierarchical ? '(SELECT resourceid FROM lv WHERE path = ?)' : '?');
      var result = makeFC(path, fc, sql);

      result.sql = (!order) ? result.sql.replace(' LIMIT', ' ORDER BY path LIMIT') : result.sql.replace(' LIMIT', ' ORDER BY creationtime ' + order + ' LIMIT');

      log.debug(result.sql, result.params);
      conn.query(result.sql, result.params, function (err, rows) {
        if (err) {
          log.debug("Error discoverying : %s ", err);
          if (callback) callback(true, err);
        }
        else {
          log.debug('Discoverying Completed.');
          if (callback) callback(false, rows);
        }
        conn.release();
      });
    }
  });
}

function makeFC(path, fc, sql) {
  var params = new Array();

  params.push(path);

  if (fc) {
    if (fc.crb != null) {
      sql += " AND creationtime < ?";
      params.push(fc.crb);
    }
    if (fc.cra != null) {
      sql += " AND creationtime >= ?";
      params.push(fc.cra);
    }
    if (fc.us != null) {
      sql += " AND lastmodifiedtime < ?";
      params.push(fc.us);
    }
    if (fc.ms != null) {
      sql += " AND lastmodifiedtime >= ?";
      params.push(fc.ms);
    }
    if (fc.sts != null) {
      sql += " AND statetag < ?";
      params.push(fc.sts);
    }
    if (fc.stb != null) {
      sql += " AND statetag >= ?";
      params.push(fc.stb);
    }
    if (fc.exb != null) {
      sql += " AND expirationtime < ?";
      params.push(fc.exb);
    }
    if (fc.exa != null) {
      sql += " AND expirationtime >= ?";
      params.push(fc.exa);
    }
    if (fc.lbl != null) {
      sql += " AND labels LIKE ?";
      params.push("%" + fc.lbl + "%");
    }
    if (fc.ty != null) {
      if (fc.ty instanceof Array) {
        sql += " AND (";
        for (var i = 0; i < fc.ty.length; i++) {
          if (i == (fc.ty.length - 1)) {
            sql += "resourcetype = ?";
          }
          else {
            sql += "resourcetype = ? OR ";
          }
          params.push(fc.ty[i]);
        }
        sql += ")";
      } else {
        sql += " AND resourcetype = ?";
        params.push(fc.ty);
      }
    }
    if (fc.szb != null) {
      sql += " AND contentsize < ?";
      params.push(fc.szb);
    }
    if (fc.sza != null) {
      sql += " AND contentsize >= ?";
      params.push(fc.sza);
    }
    if (fc.cty != null) {
      sql += " AND contentinfo = ?";
      params.push(fc.cty);
    }

    // filterCriteria 'attribute'
    var level = new exports.Level();
    for (var key in fc) {
      if (key === 'fu' || key === 'ty' || key === 'lim') {
        continue;
      }
      value = fc[key];
      if (value) {
        var attr = m2m.name.getLong(key).toLowerCase();
        level[attr] = value;
        try {
          m2m.name.getShort(key, 'FilterCrieteria');
        }
        catch (err) {
          if (attr in level) {
            if (typeof value === 'string' && value.indexOf("*") >= 0) {
              sql += " AND " + attr + " LIKE ?";
              params.push(value.replace("*", "%"));
            } else {
              sql += " AND " + attr + " = ?";
              params.push(value);
            }
          }
        }
      }
    }

    // level
    if (exports.discovery && fc.lvl != null) {
      sql += " AND f.level <= ?";
      params.push(fc.lvl);
    }

    // limit, offset
    if (fc.lim != null) {
      if (fc.ofst == null) {
        fc.ofst = 1;
      }
      sql += " LIMIT ?, ?";
      params.push(fc.ofst - 1, fc.lim);
    }
  }

  return {
    sql: sql,
    params: params
  };
}

exports.getStatus = function (callback) {
  dbPool.getConnection(function (err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    else {
      var sql = "SELECT path FROM lv WHERE path LIKE ? AND resourcetype = ?";
      // AE status container
      conn.query(sql, ['%/sts', 3], function (err, rows) {
        if (err) {
          log.debug("Error getStatus : %s ", err);
          callback(true, err);
        }
        else {
          var result = [];
          var cnt = 0;
          rows.forEach(function (row) {
            dbPool.getConnection(function (err, conn) {
              if (err) {
                log.debug('db connection failed.');
              }
              else {
                sql = "SELECT parentpath, content FROM lv WHERE parentpath = ? AND resourcetype = ? ORDER BY creationtime DESC LIMIT 1";
                // latest status
                conn.query(sql, [row.path, 4], function (err, rows) {
                  if (!err) {
                    if (rows && rows.length > 0) {
                      result.push(rows[0]);
                      log.debug('getStatus', rows[0]);
                    }
                  }
                  cnt++;
                  conn.release();
                });
              }
            });
          });

          var complete = function () {
            if (cnt === rows.length) {
              clearInterval(time);
              log.info('getStatus Completed.');
              callback(false, result);
            }
          };

          var time = setInterval(complete, 0);
        }
        conn.release();
      });
    }
  });
}

module.exports.query = (sql, arr, cb) => {
  dbPool.getConnection(function (err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    if (conn) {
      conn.query(sql, arr, function (err, rows) {
        cb(err, rows);
        conn.release();
      });
    }
  });
};

module.exports.end = (cb) => {
  if (!!dbPool) dbPool.end(cb);
  dbPool = undefined;
};
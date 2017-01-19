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

exports.dbms = 'HANA';
var hdb = require('node-hdb-pool');
var log = require('./logger').logger();

var dbConfig = {
  host: 'localhost',
  port: 30015,
  user: 'ROOT',
  password: 'Sap12346',
  maxPoolSize: 20
};

var dbPool;
exports.init = function(config, callback) {
  if (!dbPool) {
    if (config) {
      if (config.host) { dbConfig.host = config.host; }
      if (config.port) { dbConfig.port = config.port; }
      if (config.user) { dbConfig.user = config.user; }
      if (config.password) { dbConfig.password = config.password; }
      if (config.maxPoolSize) { dbConfig.maxPoolSize = config.maxPoolSize; }
    }
    dbPool = hdb.createPool(dbConfig);
    dbPool.pool.acquire(function(err, conn) {
      if (!err) dbPool.pool.release(conn);
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

exports.create = function(lv, callback) {
  var lc = lv.path.split('/').length;
  if (lc < 1) {
    log.debug('[create] Path is invalid. ' + lv.path);
    return;
  }
  dbPool.pool.acquire(function(err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    else {
      var sql = "INSERT INTO lv (";
      var first = true;
      var params = new Array();
      var value;
      for (var key in lv) {
        value = lv[key];
        if (typeof value == 'object') {
          params.push(JSON.stringify(value));
        } else {
          params.push(value);
        }
        if (first) {
          sql += "\"" + key;
          first = false;
        }
        else {
          sql += "\", \"" + key;
        }
      }
      sql += "\") VALUES (";
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
      conn.prepare(sql, function(err, statement) {
        console.log(err);
        statement.exec(params, function(err, res) {
          if (err) {
            log.debug("Error inserting : %s ", err);
            if (callback) callback(true, err);
          }
          else {
            log.info('Inserting Completed.', lv.path);
            if (callback) callback(false, res);
          }
        });
        dbPool.pool.release(conn);
      });
    }
  });
};

/*
function setLowerCaseKey(rows) {
  var cvtRows = [];
  for (var i in rows) {
    var cvtRow = {};
    for (var key in rows[i]) {
      cvtRow[key.toLowerCase()] = rows[i][key];
    }
    cvtRows.push(cvtRow);
  }
  return cvtRows;
}
*/

exports.retrieve = function(path, fc, callback) {
  var lc = path.split('/').length;
  if (lc < 1) {
    log.debug('[retrieve] Path is invalid. ' + path);
    return;
  }
  dbPool.pool.acquire(function(err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    else {
      var sql;
      if (!fc || Object.keys(fc).length == 0) { // 필터조건이 없은 경우 리소스ID로 단건 조회
        sql = "SELECT * FROM lv WHERE \"path\" = ?";
        log.debug(sql, path);
        conn.prepare(sql, function(err, statement) {
          statement.exec([path], function(err, rows) {
            if (err) {
              log.debug("Error retrieving : %s ", err);
              if (callback) callback(true, err);
            }
            else {
              log.debug('Retrieving Completed.');
              if (callback) callback(false, rows);
            }
          });
          dbPool.pool.release(conn);
        });
      }
      else {
        if (('lim' in fc) === false && ('ty' in fc) === false) {
          sql = "SELECT * FROM lv WHERE \"parentpath\" = ? AND \"resourcetype\" <> 4";
          var result = makeFC(path, fc, sql);
          log.debug(result.sql, result.params);

          var rowsWithoutCI = [];
          conn.prepare(result.sql, function(err, statement) {
            statement.exec(result.params, function(err, rows) {
              if (err) {
                log.debug("Error retrieving : %s ", err);
                if (callback) callback(true, err);
              }
              else {
                rowsWithoutCI = rows;
                dbPool.pool.release(conn);

                dbPool.pool.acquire(function(err, conn) {
                  if (err) {
                    log.debug('db connection failed.');
                  }
                  else {
                    sql = "SELECT * FROM lv WHERE \"parentpath\" = ? AND \"resourcetype\" = 4";
                    result = makeFC(path, fc, sql);
                    result.sql += " ORDER BY \"creationtime\" DESC LIMIT 1";
                    log.debug(result.sql, result.params);
                    conn.prepare(result.sql, function(err, statement) {
                      statement.exec(result.params, function(err, rows) {
                        if (err) {
                          log.debug("Error retrieving : %s ", err);
                          if (callback) callback(true, err);
                        }
                        else {
                          if (rows && rows.length > 0) {
                            rowsWithoutCI.push(rows[0]);
                          }
                          if (callback) callback(false, rowsWithoutCI);
                        }
                        log.debug('Retrieving Completed.');
                      });
                      dbPool.pool.release(conn);
                    });
                  }
                });
              }
            });
          });
        }
        else {
          sql = "SELECT * FROM lv WHERE \"parentpath\" = ?";
          var result = makeFC(path, fc, sql);
          result.sql = result.sql.replace(" LIMIT", " ORDER BY \"creationtime\" DESC LIMIT");
          log.debug(result.sql, result.params);
          conn.prepare(result.sql, function(err, statement) {
            statement.exec(result.params, function(err, rows) {
              if (err) {
                log.debug("Error retrieving : %s ", err);
                if (callback) callback(true, err);
              }
              else {
                log.debug('Retrieving Completed.');
                if (callback) callback(false, rows);
              }
            });
            dbPool.pool.release(conn);
          });
        }
      }
    }
  });
}

exports.update = function(lv, fc, callback) {
  fc = null;  // fc not supported.

  var lc = lv.path.split('/').length;
  if (lc < 1) {
    log.debug('[update] Path is invalid. ' + lv.path);
    return;
  }
  dbPool.pool.acquire(function(err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    else {
      var sql;
      var first = true;
      var params = new Array();
      var value;
      if (!fc || Object.keys(fc).length == 0) { // 필터조건이 없은 경우 리소스ID로 단건 수정
        sql = "UPDATE lv SET ";
        for (var key in lv) {
          value = lv[key];
          if (key != 'path') {
            if (typeof value == 'object') {
              params.push(JSON.stringify(value));
            } else {
              params.push(value);
            }
            if (first) {
              sql += "\"" + key + "\" = ?";
              first = false;
            }
            else {
              sql += ", \"" + key + "\" = ?";
            }
          }
        }
        sql += " WHERE \"path\" = ?";
        params.push(lv.path);
        log.debug(sql, params);
        conn.prepare(sql, function(err, statement) {
          statement.exec(params, function(err, res) {
            if (err) {
              log.debug("Error updating : %s ", err);
              if (callback) callback(true, err);
            }
            else {
              log.info('Updating Completed.', lv.path);
              if (callback) callback(false, res);
            }
          });
          dbPool.pool.release(conn);
        });
      }
      else {
        sql = "UPDATE lv SET ";
        for (var key in lv) {
          value = lv[key];
          if (typeof value == 'object') {
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
        sql += " WHERE \"path\" = ?";
        var result = makeFC(lv.path, fc, sql);
        result.params = params.concat(result.params);
        log.debug(result.sql, result.params);

        conn.prepare(result.sql, function(err, statement) {
          statement.exec(result.params, function(err, res) {
            if (err) {
              log.debug("Error updating : %s ", err);
              if (callback) callback(true, err);
            }
            else {
              log.info('Updating Completed.', lv.path);
              if (callback) callback(false, res);
            }
          });
          dbPool.pool.release(conn);
        });
      }
    }
  });
}

exports.delete = function(path, fc, callback) {
  fc = null;  // fc not supported.

  var lc = path.split('/').length;
  if (lc < 1) {
    log.debug('[delete] Path is invalid. ' + path);
    return;
  }
  dbPool.pool.acquire(function(err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    else {
      var sql;
      if (!fc || Object.keys(fc).length == 0) { // 필터조건이 없은 경우 리소스ID로 단건 삭제
        sql = "DELETE FROM lv WHERE \"path\" = ?";

        conn.prepare(sql, function(err, statement) {
          statement.exec([path], function(err, res) {
            if (err) {
              log.debug("Error deleting : %s ", err);
              if (callback) callback(true, err);
            }
            else {
              for (var i = 1; i <= (9 - lc); i++) {
                sql = "DELETE FROM lv WHERE \"path\" LIKE ?";
                var params = [path + "/%"];
                log.debug(sql, params);

                conn.prepare(sql, function(err, statement) {
                  statement.exec(params, function(err, res) {
                    if (err) {
                      log.debug("Error child deleting : %s ", err);
                      if (callback) callback(true, err);
                    }
                    else if (res.length > 0) {
                      log.debug('Child deleting Completed.');
                    }
                  });
                });
              }
              log.info('Deleting Completed.', path);
              if (callback) callback(false, res);
            }
          });
          dbPool.pool.release(conn);
        });
      }
      else {
        sql = "DELETE FROM lv WHERE \"path\" = ?"
        var result = makeFC(path, fc, sql);
        log.debug(result.sql, result.params);

        conn.prepare(result.sql, function(err, statement) {
          statement.exec(result.params, function(err, res) {
            if (err) {
              log.debug("Error deleting : %s ", err);
              if (callback) callback(true, err);
            }
            else {
              log.info('Deleting Completed.', path);
              if (callback) callback(false, res);
            }
          });
          dbPool.pool.release(conn);
        });
      }
    }
  });
}

function makeFC(path, fc, sql) {
  var params = new Array();

  params.push(path);

  if (fc) {
    if (fc.crb != null) {
      sql += " AND \"creationtime\" < ?";
      params.push(fc.crb);
    }
    if (fc.cra != null) {
      sql += " AND \"creationtime\" > ?";
      params.push(fc.cra);
    }
    if (fc.ms != null) {
      sql += " AND \"lastmodifiedtime\" < ?";
      params.push(fc.ms);
    }
    if (fc.us != null) {
      sql += " AND \"lastmodifiedtime\" > ?";
      params.push(fc.us);
    }
    if (fc.sts != null) {
      sql += " AND \"statetag\" < ?";
      params.push(fc.sts);
    }
    if (fc.stb != null) {
      sql += " AND \"statetag\" > ?";
      params.push(fc.stb);
    }
    if (fc.exb != null) {
      sql += " AND \"expirationtime\" < ?";
      params.push(fc.exb);
    }
    if (fc.exa != null) {
      sql += " AND \"expirationtime\" > ?";
      params.push(fc.exa);
    }
    if (fc.lbl != null) {
      sql += " AND \"labels\" LIKE ?";
      params.push("%" + fc.lbl + "%");
    }
    if (fc.ty != null) {
      if (fc.ty instanceof Array) {
        sql += " AND (";
        for (var i = 0; i < fc.ty.length; i++) {
          if (i == (fc.ty.length - 1)) {
            sql += "\"resourcetype\" = ?";
          }
          else {
            sql += "\"resourcetype\" = ? OR ";
          }
          params.push(fc.ty[i]);
        }
        sql += ")";
      } else {
        sql += " AND \"resourcetype\" = ?";
        params.push(fc.ty);
      }
    }
    if (fc.sza != null) {
      sql += " AND \"contentsize\" < ?";
      params.push(fc.sza);
    }
    if (fc.szb != null) {
      sql += " AND \"contentsize\" > ?";
      params.push(fc.szb);
    }
    if (fc.cty != null) {
      sql += " AND \"contentinfo\" LIKE ?";
      params.push("%" + fc.cty + "%");
    }
    if (fc.lim != null) {
      sql += " LIMIT ?";
      params.push(fc.lim);
    }
  }

  return {
    sql: sql,
    params: params
  };
}

exports.getStatus = function(callback) {
  dbPool.pool.acquire(function(err, conn) {
    if (err) {
      log.debug('db connection failed.');
    }
    else {
      var sql = "SELECT \"path\" FROM lv WHERE \"path\" LIKE ? AND \"resourcetype\" = ?";
      conn.prepare(sql, function(err, statement) {
        statement.exec(['%/sts', 3], function(err, rows) {
          if (err) {
            log.debug("Error getStatus : %s ", err);
            callback(true, err);
          }
          else {
            var result = [];
            var cnt = 0;
            rows.forEach(function(row) {
              dbPool.pool.acquire(function(err, conn) {
                if (err) {
                  log.debug('db connection failed.');
                }
                else {
                  sql = "SELECT \"parentpath\", \"content\" FROM lv WHERE \"parentpath\" = ? AND \"resourcetype\" = ? ORDER BY \"creationtime\" DESC LIMIT 1";
                  conn.prepare(sql, function(err, statement) {
                    statement.exec([row.path, 4], function(err, rows) {
                      if (!err) {
                        if (rows && rows.length > 0) {
                          result.push(rows[0]);
                          log.debug('getStatus', rows[0]);
                        }
                      }
                      cnt++;
                    });
                    dbPool.pool.release(conn);
                  });
                }
              });
            });

            var complete = function() {
              if (cnt === rows.length) {
                clearInterval(time);
                log.info('getStatus Completed.');
                callback(false, result);
              }
            };

            var time = setInterval(complete, 0);
          }
        });
        dbPool.pool.release(conn);
      });
    }
  });
}
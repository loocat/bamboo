var db = require('./db').getDB();
var log = require('./logger').logger();
var moment = require('moment');
var schedule = require('node-schedule');

var logID = 'MGMT';

exports.init = function (cse) {
  closeDB();
  scheduleManagementTasks(cse.mgmt);
}

var closeDB = () => {
  if (!!db && !!db.end) {
    db.end((err) => {
      if (err) log.error(err);
      log.debug('[%s] %s', logID, 'DB pool is closed'); 
    });
  }
}

var tasks;
var scheduleManagementTasks = function (mgmt) {

  var dbcfg = require('./conf.json')['m2m:conf'][db.dbms];

  if (db.dbms === 'MySQL') {
    dbcfg.connectionLimit = 1;
  }
  else {
    dbcfg.maxPoolSize = 1;
  }

  var jobs = {
    retain: function (period) {

      var fmtstr = 'YYYYMMDDTHHmmss'; 
      if (!period) period = { day: 21 };

      this.rule = { hour: 21, minute: 0 };
 
      this.act = () => {
        log.debug("[%s] %s", logID, 'apply retention policy');

        db.init(dbcfg, (err) => {
          if (err) log.error(err);
          else log.debug('[%s] %s', logID, 'DB pool is created');
          
          var queries = [
            // {
            //   sql: 'SELECT resourceid, parentpath from lv where resourcetype = 4 and resourcename like "CI%" and lastmodifiedtime < ?',
            //   parameters: [ moment().startOf('minute').subtract(period).format(fmtstr) ],
            // },
            {
              sql: 'DELETE from lv where resourcetype = 4 and resourcename like "cin-%" and lastmodifiedtime < ?',
              parameters: [ moment().startOf('minute').subtract(period).format(fmtstr) ],
              callback: (err, res) => {
                if (!err && !!res && res.affectedRows > 0) {
                  log.info('[%s] removed %d old contentInstance(s)', logID, res.affectedRows);
                }
              }
            },
            // {
            //   sql: 'SELECT resourceid, parentpath from lv where expirationtime is not NULL and expirationtime < ?',
            //   params: [ moment().startOf('minute').format(fmtstr) ],
            // },
            {
              sql: 'DELETE from lv where expirationtime is not NULL and expirationtime < ?',
              parameters: [ moment().startOf('minute').format(fmtstr) ],
              callback: (err, res) => {
                if (!err && !!res && res.affectedRows > 0) {
                  log.info('[%s] removed %d expired contentInstance(s)', logID, res.affectedRows);
                }
              }
            }
          ];

          var cnt = queries.length;

          queries.forEach((e) => {
            db.query(e.sql, e.parameters, (err, res) => {
              if (err) {
                log.debug(e);
                log.error(err);
              }
              
              // if (!!res) {
              //   if (Array.isArray(res)) {
              //     res.forEach((row) => {
              //       log.debug(row.resourceid + '\t' + row.parentpath); 
              //     });
              //     log.debug('length:', res.length);
              //   }
              //   else {
              //     log.debug(res);
              //   }
              // }

              if (!!e.callback) {
                e.callback(err, res);
              }

              if (--cnt < 1) {
                closeDB();
              } 
            });
          });
        });
      };
    },
    ping: function (period) {
      this.rule = { second: new schedule.Range(0, 50, 10) };
      console.log(this.rule);
      this.act = () => { console.log(moment.now()); }
      // gather POA of in-cse and mn-cse(s)
      // retrieve attributes of each cse via http binding
    }
  };

  var rules = mgmt.rules;
  if (!tasks) tasks = {};

  for (var ii in jobs) {
    if (rules && rules[ii]) {
      var job = new jobs[ii](rules[ii]);
      if (!tasks[ii]) {
        tasks[ii] = schedule.scheduleJob(job.rule, job.act);
      }
      else {
        tasks[ii].reschedule(job.rule, job.act);
      }
    }
    else if (!!tasks[ii]) {
      tasks[ii].cancel();
      delete tasks[ii];
    }
  }
}

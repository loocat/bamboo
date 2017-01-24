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

var async = require('async');
var crud = require('./crud');
var m2m = require('onem2m');
var url = require('url');
var util = require('util');
var log = require('./logger').logger();

var checkEventNotificationCriteria = function (op, enc, msg) {
  if (enc && enc.net) {
    switch (+enc.net) {
    case m2m.code.getNotificationEventCriteria('Update_of_Resource'):
      if (op === m2m.code.getOperation('Update')) return true;
      break;
    case m2m.code.getNotificationEventCriteria('Delete_of_Resource'):
      if (op === m2m.code.getOperation('Delete')) return true;
      break;
    case m2m.code.getNotificationEventCriteria('Create_of_Direct_Child_Resource'):
      if (op === m2m.code.getOperation('Create')) return true;
      break;
    case m2m.code.getNotificationEventCriteria('Delete_of_Direct_Child_Resource'):
      if (op === m2m.code.getOperation('Delete')) return true;
      break;
    case m2m.code.getNotificationEventCriteria('Retrieve_of_Container_Resource_With_No_Child_Resource'):
      if (op === m2m.code.getOperation('Retrieve')) return true;
      break;
    }
  }

  if (enc && enc.atr) {
    for (var ii in enc.atr) {
      if (enc.atr[ii]) {

        // log.debug(ii + '\t' + JSON.stringify(enc.atr[ii]));
        var arr = enc.atr[ii];
        if (!Array.isArray(arr)) {
          arr = [arr];
        }
        
        if (ii === 'ty') {
          // log.debug('check resourceType: ' + msg.ty);
          var match = false;
          for (var kk = 0; kk < arr.length; ++kk) {
            // if (msg[m2m.name.getShort(m2m.getResourceType(Number.parseInt(arr[kk])), 'ResourceTypes')]) {
            if (msg.ty === +arr[kk]) {              
              match = true;
              break;
            }
          }
          if (match === false) { return false; }
          // log.debug('ok');
        }
        else if (msg.lbl && ii === 'lbl') {
          // log.debug('check labels: ' + msg.lbl);
          var match = false;
          for (var kk = 0; kk < arr.length; ++kk) {
            if (msg.lbl.indexOf(arr[kk]) > -1) {
              match = true;
              break;
            }
          }
          if (match === false) { return false; }
          // log.debug('ok');
        }
        else {
          // TODO ...
        }
      }
    }

    // if 'attribute' is not empty, at least one 'operationMonitor:operation' shall be present 
    var om = enc.om;
    if (!om) { return false; }

    var opr = om.opr;
    if (!opr) { return false; }

    // log.debug('check operationMonitor: ' + opr);
    // log.debug(JSON.stringify(msg, null, '  '));
    var arr = opr;
    if (!Array.isArray(arr)) {
      arr = [arr];
    }
    var match = false;
    for (var kk in arr) {
      if (Number.parseInt(arr[kk]) === op) {
        match = true;
        break;
      }
    }
    if (match === false) { return false; }
    // log.debug('ok');
  }

  return true;
}

exports.postprocess = function (rqp, rsp) {

  if (!rsp.fr) return;

  var sendNotification = function (sub, path, msg) {
    for (var ii in msg) {
      if (sub && sub.enc && checkEventNotificationCriteria(rqp.op, sub.enc, msg[ii])) {
        var nu = sub['nu'] || sub['notificationURI'];
        if (!nu || nu.length < 1) return; 
        nu = nu.split(' ');
        if (nu.length < 1) return;

        msg.rss = rsp.rsc;
        msg = { nev: msg, sur: util.format('/%s/%s', require('./protocol').getCseID(), sub.ri) };
        msg = m2m.util.wrapMessage(m2m.name.getShort('notification'), msg, rqp.ac || rqp.cty);
        var noti = {
          rqi: m2m.util.createRequestID(),
          op: m2m.code.getOperation('Notify'),
          to: path,
          cty: rqp.ac || rqp.cty,
          pc: msg
        };

        var fire = (poa, idx, cb) => {
          var parsedUrl = url.parse(poa);
          var target = parsedUrl.pathname;
          if (target && target.length > 1) {
            target = target.replace(/^\//, '');
            if (target !== sub.cr) {
              log.warn('[%s] inconsistent resource id in \'pointOfAccess\': %s', sub.ri, poa);
            }
          }
          else if (sub.cr && sub.cr.length > 0) {
            parsedUrl.pathname = m2m.util.addr2path(sub.cr);
            poa = url.format(parsedUrl);
            log.debug('[%s] override \'pointOfAccess\': %s', sub.ri, poa);
          }

          var agent = require('./protocol').getAgent(parsedUrl.protocol);
          if (!agent) {
            log.error('[%s] unknown binding protocol: %s', sub.ri, poa);
            cb(poa);
          }
          else {
            agent.sendRQP(poa, noti, cb);
          }
        };

        async.eachOfSeries(nu, fire, (err, res) => {
          if (err) {
            log.warn('[%s] no response%s: %s', sub.ri, sub.cr ? ' from ' + sub.cr : '', nu.join(' '));
          }
        });
      }
    }
  }
  
  // send notifications
  var sendNotifications = function (subs, msg) {
    if (Array.isArray(subs) === false) subs = [subs];
    for (var el in subs) {
      // log.debug('-----------\n' + JSON.stringify(subs[el], null, ' '));
      sendNotification(subs[el].sub, rsp.fr, msg);
    }
  }

  // get resource
  var tmpReq = {
    to: rsp.fr,
    op: m2m.code.getOperation('Retrieve')
  };

  crud.operation(tmpReq, function (rsp) {

    if (rsp.rsc !== m2m.code.getResponseStatusCode('OK')) {
      // log.debug('failed retrieve ' + tmpReq.to);
      return;
    }

    var msg = (Array.isArray(rsp.pc) && rsp.pc.length === 1) ? rsp.pc[0] : rsp.pc;
    if (msg.path) delete msg.path;
    
    // get subscriptions
    tmpReq.to = rqp.to;
    tmpReq.fc = { ty: m2m.code.getResourceType('subscription') };
    
    crud.operation(tmpReq, function (rsp) {
      if (rsp.rsc === m2m.code.getResponseStatusCode('OK') && rsp.pc) {
        // fire notifications
        sendNotifications(rsp.pc, msg);
      }
    });
  });
}

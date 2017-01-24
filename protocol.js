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

'use strict';

var async = require('async');
var xml2js = require('xml2js');
var crud = require('./crud');
var m2m = require('onem2m');
var url = require('url');
var util = require('util');
var events = require('events');
var log = require('./logger').logger();

/**
 * 
 */
var getChildObject = function (parentObject, childName, format) {
  var prefix = (format === 'xml') ? 'm2m:' : '';
  var shortName = prefix + m2m.name.getShort(childName, 'ResourceTypes');
  var longName = prefix + m2m.name.getLong(childName, 'ResourceTypes');
  return (parentObject[shortName] || parentObject[longName]);
}

/**
 * 
 */
var buildM2MResponse = function (options, content) {
  var rsp = {};
  for (var i in options) {
    rsp[i] = options[i];
  }
  if (content) rsp.pc = content;
  var message = m2m.util.wrapMessage('responsePrimitive', rsp, options.cty);
  if (typeof message !== 'string') {
    message = JSON.stringify(message);
  }
  return rsp;
}

var getLocalAddress = (addr, cseid, spid) => {
  var ttt = getCSERelativeAddress(addr, cseid || cseID, spid || spID);
  if (m2m.util.isStructured(ttt)) {
    return ttt.split('/').slice(1).join('/');
  }
  throw new Error('[UNSTRUCTURED] %s', addr);
}

/**
 * 
 */
var getCSERelativeAddress = (addr, cseid, spid) => {
  return m2m.util.getCSERelativeAddress(addr, cseid || cseID, spid || spID);
}

/**
 * 
 */
var getSPRelativeAddress = (addr, cseid, spid) => {
  return m2m.util.getSPRelativeAddress(addr, cseid || cseID, spid || spID);
}

/**
 * 
 */
var getAbsoluteAddress = (addr, cseid, spid) => {
  return m2m.util.getAbsoluteAddress(addr, cseid || cseID, spid || spID);
}

/**
 * 
 */
var ResourceCollector = function (rqp, rootPath) {

  if (!rootPath) {
    rootPath = rqp.to;
  }

  var self = this;
  var pending = 0;
  var content;
  var fcLimitted;
  var unstructured = (rqp.drt === m2m.code.getDiscResType('unstructured'));


  if (rqp.fc) {
    fcLimitted = { lim: 1 };
    for (var ii in rqp.fc) {
      fcLimitted[ii] = rqp.fc[ii];
    }
  }

  var isLimittedResource = function (rsc) {
    // return (
    //   rsc.ty === m2m.code.getResourceType('container') &&
    //   (rsc.rn === 'dat' || rsc.rn === 'sts' || rsc.rn === 'exe' || rsc.rn === 'inf')
    // );
    return false;
  }

  var leaf = [m2m.code.getResourceType('contentInstance'), m2m.code.getResourceType('subscription')];
  var isLeaf = function (ty) {
    for (var ii in leaf) {
      if (leaf[ii] === ty) return true;
    }
    return false;
  }

  var dive = 1; // direct children only 

  if (rqp.rcn === m2m.code.getResultContent('Attributes') ||
    rqp.rcn === m2m.code.getResultContent('Hierarchical address') ||
    rqp.rcn === m2m.code.getResultContent('Hierarchical address and attributes')) {
    dive = 0; // stop collecting child
  }

  var address;
  if (rqp.rcn === m2m.code.getResultContent('Attributes and child resource references') ||
    rqp.rcn === m2m.code.getResultContent('Child resource references')) {
    address = [];
  }

  var accept = () => { return true; };

  if (rqp.fc) {
    if ('ty' in rqp.fc) {
      // replace accept()
      var _ty = rqp.fc.ty;
      if (Array.isArray(_ty)) {
        accept = (ty) => { return (_ty.indexOf(ty) > -1); };
      }
      else {
        accept = (ty) => { return (_ty === ty); };
      }
      // delete rqp.fc.ty;
    }

    // if ('lim' in rqp.fc) {
    //   delete rqp.fc.lim;
    // }
  }

  if (rqp.op === m2m.code.getOperation('Retrieve')) {
    if (rqp.fc && rqp.fc.fu === m2m.code.getFilterUsage('Discovery Criteria')) {
      dive = -1; // no limit
      rqp.rcn = m2m.code.getResultContent('Child resource references');
      if (!address) {
        address = [];
      }
    }
    else if (rqp.rcn === m2m.code.getResultContent('Child resources') ||
      rqp.rcn === m2m.code.getResultContent('Attributes and child resources')) {
      dive = -1; // no limit
    }
  }

  var collect = function (container, parentPath, fc) {

    if (!parentPath) {
      parentPath = rootPath;
    }

    var relativePath = parentPath.replace(rootPath, '.');
    var relativeDepth = relativePath.split('/').length;
    if (!relativeDepth) relativeDepth = 0;
    log.debug('dive  = ' + dive);
    log.debug('depth = %d, %s/%s', relativeDepth, parentPath, relativePath);

    var append = (key, val) => {
      let tmp = container[key];
      if (!tmp) container[key] = [val];
      else {
        if (!Array.isArray(tmp)) {
          tmp = [tmp];
        }
        tmp.push(val);
      }
    };

    var travel = function (rsp) {
      if (dive !== 0 && rsp.pc && rsp.pc.length > 0) {
        rsp.pc.map(obj => {
          let key = Object.keys(obj)[0];
          obj = obj[key];

          let path;
          if (!obj.path) {
            path = parentPath + '/' + obj.rn;
          }
          else {
            path = obj.path;
            delete obj.path;
          }

          if (address) {
            if (accept(obj.ty)) {
              address.push('/' + cseID + '/' + (unstructured ? obj.ri : path));
            }
          }
          else {
            append(key, obj);
          }

          if ((dive === -1) || (relativeDepth < dive)) {
            if (isLeaf(obj.ty) === false) {
              collect(obj, path, isLimittedResource(obj) ? fcLimitted : fc);
            }
          }
        });
      }

      pending--;
      if (parentPath === rootPath) {
        var terminate = function () {
          if (pending < 1) {
            clearInterval(sss);
            self.finish();
          }
        }
        var sss = setInterval(terminate, 1);
      }
    };

    var discovery = (rsp) => {
      if (rsp.rsc === m2m.code.getResponseStatusCode('OK')) {
        address = rsp.pc.map(addr => '/' + cseID + '/' + addr);
      }
      self.finish();
    }

    pending++;
    crud.operation({
        op: m2m.code.getOperation('Retrieve'),
        to: parentPath,
        fc: fc,
        drt: rqp.drt,
        dst: rqp.dst
      },
      (fc && fc.fu === m2m.code.getFilterUsage('Discovery Criteria') && crud.discovery) ? discovery : travel
    );
  };

  var rootResourceType;

  var start = function () {
    if (rqp.rcn === m2m.code.getResultContent('Nothing')) {
      process.nextTick(self.finish);
    }
    else crud.operation({ op: m2m.code.getOperation('Retrieve'), to: rootPath }, function (rsp) {
      if (rsp.rsc === m2m.code.getResponseStatusCode('OK') && rsp.pc.length === 1) {
        content = rsp.pc[0];
        for (var rr in content) {
          rootResourceType = rr;
          if (rqp.rcn === m2m.code.getResultContent('Attributes') ||
            rqp.rcn === m2m.code.getResultContent('Hierarchical address and attributes') ||
            rqp.rcn === m2m.code.getResultContent('Attributes and child resources') ||
            rqp.rcn === m2m.code.getResultContent('Attributes and child resource references')) {
            //
            // keep public attributes
            //
            if (content[rr].path) delete content[rr].path;
          }
          else {
            //
            // discard attributes
            //
            content[rr] = {};
          }
        }

        if (rqp.op === m2m.code.getOperation('Create')) {
          content.hierarchicalAddress = rootPath;
        }

        collect(content[rootResourceType], rootPath, rqp.fc);
      }
      else {
        self.finish();
      }
    });
  };

  this.finish = function () {
    if (content && rootResourceType) {
      if (address) {
        rootResourceType = m2m.name.getShort('URIList');
        content[rootResourceType] = address.join(' ');
      }
      content = m2m.util.wrapMessage(rootResourceType, content[rootResourceType], (rqp.ac || rqp.cty), rqp.nmtype === 'long');
    }
    self.emit('done', content);
  }

  events.call(this);
  start();
}
util.inherits(ResourceCollector, events);

/**
 * 
 */
var collectResource = function (rqp, rootPath, callback) {
  (new ResourceCollector(rqp, rootPath)).on('done', callback);
}

/**
 * 
 */
var path2id = (path, callback) => {
  crud.operation({ op: m2m.code.getOperation('Retrieve'), to: path }, (rsp) => {
    var id = undefined;
    if (rsp.rsc === m2m.code.getResponseStatusCode('OK') && rsp.pc.length === 1) {
      var keys = Object.keys(rsp.pc[0]);
      if (keys.length === 1) {
        id = rsp.pc[0][keys[0]].ri;
      }
    }
    callback(id);
  });
}

/**
 * 
 */
var id2path = (id, callback) => {
  let rqp = { op: m2m.code.getOperation('Retrieve'), to: id };
  crud.operation(rqp, (rsp) => {
    if (rsp.rsc !== m2m.code.getResponseStatusCode('OK')) {
      callback(rsp);
    }
    else {
      let obj = getResource(rsp);
      if (!obj.pi) {
        // is a CSE
        callback(null, obj.rn);
      }
      else {
        rqp.to = obj.pi;
        rqp.rcn = m2m.code.getResultContent('Child resource references');
        rqp.fc = { fu: m2m.code.getFilterUsage('Discovery Criteria'), lvl: 1 };
        crud.operation(rqp, (rsp) => {
          let found = false;
          if (rsp.rsc === m2m.code.getResponseStatusCode('OK') && rsp.pc) {
            rsp.pc.map(e => {
              let path = (typeof e === 'string') ? e : e[Object.keys(e)[0]].path;
              if (path.split('/').pop() === obj.rn) {
                callback(null, path);
                found = true;
              }
            });
          }
          if (!found) {
            // throw new Error(m2m.code.getResonseStatusCode(rsp.rsc));
            callback(rsp);
          }
        });
      }
    }
  });
}

/**
 * extract resource from response primitive
 */
var getResource = (rsp) => {
  var obj = rsp.pc[0];
  return obj[Object.keys(obj)[0]];
};

/**
 * Policy Decision Point (PDP)
 * 
 * see TS-0003-v1.4.2 clause 6.2.2
 */
var policyDecisionPoint = (env, cb) => {

  var evaluate = (acr, cb) => {
    var grant = true;
    if (acr.acor) acr.acor = acr.acor.replace(/\s+/g, ' ');
    if (acr.acop) acr.acop = +acr.acop;

    if (acr.acor) {
      // validate accessControlOriginators
      var orgs = acr.acor.split(' ');
      if (orgs.indexOf('*') < 0 && orgs.indexOf(env.rqp.fr) < 0) {
        grant = false;
      }
    }
    if (grant && acr.acop) {
      // validate accessControlOperations
      if ((acr.acop & env.rqp.op) === 0) {
        grant = false;
      }
    }
    if (grant && acr.acco) {
      // TODO validate accessControlContexts
    }
    cb(null, grant);
  }

  var retrieve = (id, cb) => {
    crud.operation({ op: m2m.code.getOperation('Retrieve'), to: id }, function (rsp) {
      if (rsp.rsc !== m2m.code.getResponseStatusCode('OK') || rsp.pc.length !== 1) {
        // resource does not exist or not unique
        log.error('NOT_EXIST: acp ', id);
        cb(null, false);
      }
      else {
        var privileges = m2m.name.getShort(env.self ? 'selfPrivileges' : 'privileges');
        async.some(getResource(rsp)[privileges], evaluate, (err, res) => {
          cb(err, !err && res === true);
        });
      }
    });
  }

  if (env.obj.acpi) {
    async.some(env.obj.acpi.split(' '), retrieve, (err, res) => {
      cb(!err && res === true);
    });
  }
  else {
    // not grant access (default)
    cb(false);
  }
}

/**
 * Policy Enforcement Point (PEP)
 * 
 * see TS-0003-v1.4.2 clause 6.2.2
 */
var policyEnforcementPoint = (env, cb) => {
  var obj = env.obj;
  if (typeof env.self === 'undefined') {
    // set initial value of env.self
    env.self = (obj.ty === m2m.code.getResourceType('accessControlPolicy'));
  }
  if (obj.acpi) {
    // found 'accesControlPolicyIDs' attribute
    policyDecisionPoint(env, cb);
  }
  else if (obj.pi) {
    // retrieve parent resource by its id
    crud.operation({ op: m2m.code.getOperation('Retrieve'), to: obj.pi }, function (rsp) {
      if (rsp.rsc !== m2m.code.getResponseStatusCode('OK') || rsp.pc.length !== 1) {
        // resource does not exist or not unique
        log.error('NOT_EXIST: parent', obj.pi);
        cb(false);
      }
      else {
        // set the parent resoure as env.obj 
        env.obj = getResource(rsp);
        policyEnforcementPoint(env, cb);
      }
    });
  }
  else {
    // CSEBase has no parent
    cb(true);
  }
}

/**
 * 
 */
var handleRequestPrimitive = function (msg) {

  var rqp = msg.rqp;
  var agent = msg.agent;
  var topic = msg.topic;

  log.info('[%s] %s', rqp.fr, rqp.rqi);

  var adjustRSP = (rsp) => {
    rsp.fr = cseID;
    if (!rsp.to) rsp.to = rqp.fr;
    return rsp;
  };

  var getMessageSource = (src) => {
    if (topic) return topic.split('/')[3]; // topic format: /oneM2M/req/{SOURCE}/{TARGET}/{MIME_TYPE}
    return src;
  };

  var sendErrorResponse = function (rsc, msg) {
    var rsp = {
      rqi: rqp.rqi,
      rsc: rsc,
      pc: { code: rsc, message: msg }
    };
    var pcType = rqp.ac || rqp.cty;
    if (pcType === 'xml') {
      rsp.pc = (new xml2js.Builder({ rootName: 'error' })).buildObject(rsp.pc);
    }
    else {
      rsp.pc = { error: rsp.pc };
    }
    agent.sendResponse(getMessageSource(rqp.fr), adjustRSP(rsp));
    log.error('[%s] %s', logID, JSON.stringify(rsp.pc));
  };

  var handleLatestOrOldest = function (rqp, callback) {
    var arr = rqp.to.split('/');
    var target = arr.pop();
    var tmp = {
      op: m2m.code.getOperation('Retrieve'),
      to: arr.join('/'),
      rcn: m2m.code.getResultContent('Child resource references'),
      dst: m2m.code.getSortType(target === 'la' ? 'Descending' : 'Ascending'),
      fc: {
        fu: m2m.code.getFilterUsage('Discovery Criteria'),
        ty: m2m.code.getResourceType('contentInstance'),
        lim: 1,
        lvl: 1
      }
    };
    crud.operation(tmp, function (rsp) {
      if (rsp && rsp.pc && rsp.pc.length > 0) {
        let eee = rsp.pc[0];
        if (typeof eee !== 'string') {
          eee = eee[Object.keys(eee)[0]].path;
        }
        rqp.to = getLocalAddress(eee);
        rqp.ty = tmp.fc.ty;
      }
      callback(rqp);
    });
  }

  var preprocess = function (rqp, callback) {

    var check = function () {
      var err = require('./check').checkRequest(rqp);
      if (err) {
        log.error('[%s] %s', logID, err);
        sendErrorResponse(m2m.code.getResponseStatusCode('BAD_REQUEST'), err);
      }
      else if (rqp.to.match(/\/(la|ol)$/)) {
        handleLatestOrOldest(rqp, callback);
      }
      else {
        callback(rqp);
      }
    };

    var single = (rsp) => {
      var obj = getResource(rsp);
      if (rqp.op == m2m.code.getOperation('Delete')) {
        // set parent resource type
        rqp.pty = obj.ty;

        if (!rqp.pty) {
          // cannot determine the type of parent resource
          sendErrorResponse(m2m.code.getResponseStatusCode('BAD_REQUEST'), 'not exact parent resource path: ' + path);
        }
        else {
          check();
        }
      }
      else if (rqp.op === m2m.code.getOperation('Create')) {
        //
        // we should guarantee that the parent resource can hold the child resource
        //

        // set parent resource type
        rqp.pty = obj.ty;

        if (!rqp.pty) {
          // cannot determine the type of parent resource
          sendErrorResponse(m2m.code.getResponseStatusCode('BAD_REQUEST'), 'not exact resource path: ' + path);
        }
        else if (!require('./check').isPossibleChild(rqp.pty, rqp.ty)) {
          // the parent resource cannot hold the child resouce
          sendErrorResponse(m2m.code.getResponseStatusCode('BAD_REQUEST'), !rqp.ty ? 'empty resource type' : 'not allowed resource type: ' + m2m.code.getResourceType(rqp.ty));
        }
        else {
          // okay. go on
          check();
        }
      }
      else {
        // check resource types
        if (rqp.ty && rqp.ty > 0) {
          if (typeof rqp.pc[m2m.name.getShort(m2m.code.getResourceType(rqp.ty), 'ResourceTypes')] === 'undefined') {
            // retrieved information has different resource structure
            delete rqp.ty;
          }
        }
        else {
          rqp.ty = obj.ty;
        }
        check();
      }
    }

    var multiple = (rsp) => {
      if (getResource(rsp).ty !== m2m.code.getResourceType('group')) {
        sendErrorResponse(m2m.code.getResponseStatusCode('BAD_REQUEST'), 'not allowd resource type: ' + m2m.name.getLong(m2m.code.getResourceType(getResourceType(rsp.pc[0]))));
      }
      else {
        var arr = [];

        var wrap = (root, msg, ac) => {
          return m2m.util.wrapMessage(root, msg, (ac || rqp.ac || rqp.cty), rqp.nmtype === 'long');
        }

        var aggregate = (err, rsp) => {
          if (rsp) {
            delete rsp.rqi;
            if (rsp.pc && typeof rsp.pc === 'string') {
              rsp.pc = m2m.util.parseJson(rsp.pc);
            }
          }
          arr.push(err || rsp);
          if (arr.length === mem.length) {
            var agr = {
              rqi: rqp.rqi,
              rsc: m2m.code.getResponseStatusCode('OK'),
              pc: wrap('aggregatedResponse', { rsp: arr })
            };
            sendResponse(agr);
          }
        }
        var grp = rsp.pc[0][m2m.name.getShort('group')];
        var mem;
        if (grp) mem = grp[m2m.name.getShort('memberIDs')].split(' ');
        if (mem) mem.forEach((memberID) => {
          memberID = memberID.trim();
          var ttt = JSON.parse(JSON.stringify(rqp));
          ttt.to = memberID;
          ttt.rqi = m2m.util.createRequestID();
          if (ttt.cty) delete ttt.cty;
          ttt.ac = 'json';
          mqttAgent.sendRQP(undefined, ttt, aggregate);
        });
      }
    }

    // shorten virtual resource names
    rqp.to = rqp.to.replace(/\/latest$/, '/la');
    rqp.to = rqp.to.replace(/\/oldest$/, '/ol');
    rqp.to = rqp.to.replace(/\/fanOutPoint$/, '/fopt');

    var path = rqp.to;
    var fopt = false;
    if (rqp.to.match(/\/fopt$/)) {
      path = path.replace(/\/[^\/]+$/, '');
      fopt = true;
    }
    else {
      if (rqp.op === m2m.code.getOperation('Retrieve')) {
        path = path.replace(/\/(la|ol)$/, '');
      }
      else if (rqp.op === m2m.code.getOperation('Delete')) {
        path = path.replace(/\/[^\/]+$/, '');
      }
    }

    crud.operation({ op: m2m.code.getOperation('Retrieve'), to: path }, function (rsp) {
      if (rsp.rsc !== m2m.code.getResponseStatusCode('OK') || rsp.pc.length === 0) {
        // resource does not exist
        sendErrorResponse(rsp.rsc, 'cannot find the resource path: ' + path);
      }
      else if (rsp.pc.length !== 1) {
        // resource is not unique
        rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
        sendErrorResponse(rsp.rsc, 'not exact resource path: ' + path);
      }
      else {
        policyEnforcementPoint({ obj: getResource(rsp), rqp: rqp }, (granted) => {
          if (!granted) {
            sendErrorResponse(m2m.code.getResponseStatusCode('ORIGINATOR_HAS_NO_PRIVILEGE'), 'access not granted');
          }
          else {
            (fopt ? multiple : single)(rsp);
          }
        });
      }
    });
  };

  var sendResponse = function (rsp, success) {
    var src = getMessageSource(rsp.to);
    if (success) {
      if (getCSERelativeAddress(rqp.to)) {
        var ty = (rqp.op === m2m.code.getOperation('Create') || rqp.op === m2m.code.getOperation('Delete')) ? rqp.pty : rqp.ty;
        if (typeof ty !== 'undefined' && require('./check').isPossibleChild(ty, m2m.code.getResourceType('subscription'))) {
          require('./notify').postprocess(rqp, rsp);
        }
        if (rqp.op !== m2m.code.getOperation('Delete')) {
          if (!rqp.fc) rqp.fc = { fu: 0 };
          collectResource(rqp, rsp.fr, function (responseContent) {
            if (responseContent) rsp.pc = responseContent;
            agent.sendResponse(src, adjustRSP(rsp));
          });
        }
        else {
          agent.sendResponse(src, adjustRSP(rsp));
        }
      }
      else {
        agent.sendResponse(src, adjustRSP(rsp));
      }
    }
    else {
      agent.sendResponse(src, adjustRSP(rsp));
    }
  };

  var mainprocess = function (rqp) {

    if (rqp.op === m2m.code.getOperation('Notify')) {
      //
      // TS_118.101
      // 9.3.2.3 Notification Re-targeting
      // 9.3.2.3.1 Application Entity Point of Access (AE-PoA)
      // A Notify request to an AE is sent by targeting <AE> resource on a
      // Hosting CSE. If the Hosting CSE verifies access control privilege
      // of the Originator, the Hosting CSE shall re-target the request to
      // the address specified as AE-PoA (i.e. pointOfAccess attribute of
      // <AE> resource). The AE-PoA may be initially configured in the <AE>
      // resource when the AE registers to the Registrar CSE. If the <AE>
      // resource does not contain an AE-PoA, an active communication link,
      // if available, can be used for re-targeting. If neither of them is
      // available, the request cannot be re-targeted to the AE.
      //
      if (rqp.ty !== m2m.code.getResourceType('AE')) {
        sendErrorResponse(m2m.code.getResponseStatusCode('BAD_REQUEST'), '\'Notify\' operation is not applicable to \'' + m2m.code.getResourceType(rqp.ty) + '\' resource');
      }
      else {
        // TODO implementation
      }
    }
    else if (rqp.op === m2m.code.getOperation('Create')) {
      let createChild = () => {
        crud.operation(rqp, (rsp) => {
          sendResponse(rsp, rsp.rsc === m2m.code.getResponseStatusCode('CREATED'));
        });
      };
      // check uniqueness of the resourceName
      let rn = rqp.pc[m2m.name.getShort(m2m.code.getResourceType(rqp.ty))].rn;
      if (rn) {
        let tmp = {
          op: m2m.code.getOperation('Retrieve'),
          to: rqp.to + '/' + rn
        };
        crud.operation(tmp, function (rsp) {
          if (rsp.rsc === m2m.code.getResponseStatusCode('OK')) {
            sendErrorResponse(m2m.code.getResponseStatusCode('ALREADY_EXISTS'), 'resource exists: ' + tmp.to);
          }
          else {
            createChild();
          }
        });
      }
      else {
        createChild();
      }
    }
    else if (rqp.op === m2m.code.getOperation('Retrieve')) {
      // the resource path is identified beforehand 
      var rsp = {
        rqi: rqp.rqi,
        fr: rqp.to,
        to: rqp.fr,
        rsc: m2m.code.getResponseStatusCode('OK')
      };
      sendResponse(rsp, true);
    }
    else if (rqp.op === m2m.code.getOperation('Update')) {
      crud.operation(rqp, function (rsp) {
        sendResponse(rsp, rsp.rsc === m2m.code.getResponseStatusCode('OK'));
      });
    }
    else if (rqp.op === m2m.code.getOperation('Delete')) {
      crud.operation(rqp, function (rsp) {
        sendResponse(rsp, rsp.rsc === m2m.code.getResponseStatusCode('OK'));
      });
    }
    else {
      sendErrorResponse(m2m.code.getResponseStatusCode('BAD_REQUEST'), 'unknown operation: ' + rqp.op);
    }
  };

  var forward = (url, rqp) => {
    log.info('[%s] forward to %s', logID, rqp.to);
    if (!url || url.length < 1) {
      log.error('[%s] invalid url %s', logID, rqp.rqi);
      return;
    }
    rqp.cty = 'json';
    getAgent(url).sendRQP(url, rqp, (err, rsp) => {
      if (err) {
        log.error(err);
        sendErrorResponse(m2m.code.getResponseStatusCode('REQUEST_TIMEOUT'), JSON.stringify(err));
      }
      else {
        sendResponse(rsp, rsp.rsc === m2m.code.getResponseStatusCode('OK'));
      }
    });
  };

  if (!getCSERelativeAddress(rqp.to)) {
    // forward message to other CSEBase
    if (cseType === m2m.code.getCseTypeID('IN_CSE')) {
      var ttt = {
        op: m2m.code.getOperation('Retrieve'),
        fr: cseID,
        to: csePath,
        rcn: m2m.code.getResultContent('Child resource references'),
        fc: {
          ty: m2m.code.getResourceType('remoteCSE')
        }
      };
      crud.operation(ttt, (rsp) => {
        log.info('[%s] %s %s... %s', logID, m2m.code.getOperation(ttt.op), ttt.to, m2m.code.getResponseStatusCode(rsp.rsc));
        var csr;
        if (rsp.rsc === m2m.code.getResponseStatusCode('OK')) rsp.pc.forEach((obj) => {
          if (Object.keys(obj).indexOf(m2m.name.getShort('remoteCSE')) < 0) return;
          if ('/' + obj.csr.csi + '/' + obj.csr.rn === rqp.to) {
            // SP-Relative case only
            csr = obj.csr;
          }
        });
        if (csr) {
          var poa = csr.poa.split(' ')[0]; // TODO support multiple poa
          rqp.to = getSPRelativeAddress(rqp.to, csr.csi);
          forward(csr.poa.split(' ')[0] + '/' + csr.csi, rqp);
        }
        else {
          var err = 'not registered remoteCSE:' + csePath;
          log.error(err);
          sendErrorResponse(m2m.code.getResponseStatusCode('TARGET_NOT_REACHABLE'), JSON.stringify(err));
        }
      });
    }
    else if (incse) {
      // forward to IN_CSE
      forward(incse.poa.split(' ')[0] + '/' + incse.csi, rqp);
    }
  }
  else {
    let afterPreprocess = () => {
      if (rqp.to) {
        mainprocess(rqp);
      }
      else {
        // in case of accessing 'latest' or 'oldest' on an empty container
        sendErrorResponse(m2m.code.getResponseStatusCode('NOT_FOUND'), '\'container\' has no child \'contentInstance\' resource');
      }
    };

    // get the local path of the resource
    rqp.to = getLocalAddress(rqp.to);

    // transform resource_id to resource_path
    let arr = rqp.to.split('/');
    if (m2m.util.isID(arr[0])) {
      id2path(arr[0], (err, path) => {
        if (err) {
          sendErrorResponse(m2m.code.getResponseStatusCode('NOT_FOUND', 'unknown resource id ' + arr[0]));
        }
        else {
          arr[0] = path;
          rqp.to = arr.join('/');
          preprocess(rqp, afterPreprocess);
        }
      });
    }
    else {
      preprocess(rqp, afterPreprocess);
    }
  }
};

var httpAgent;
var mqttAgent;
var cseType;
var csePath;
var logID;
var cseID;
var spID;
var incse;

/**
 * 
 */
var getAgent = function (url) {
  if (url.match(/^http/)) return httpAgent;
  if (url.match(/^mqtt/)) return mqttAgent;
  return null;
}

/**
 * 
 */
var getSrt = function () {
  return [
    m2m.code.getResourceType('AE'),
    m2m.code.getResourceType('container'),
    m2m.code.getResourceType('contentInstance'),
    m2m.code.getResourceType('CSEBase'),
    m2m.code.getResourceType('remoteCSE'),
    m2m.code.getResourceType('subscription'),
    m2m.code.getResourceType('group'),
    m2m.code.getResourceType('accessControlPolicy'),
  ].sort((a, b) => { return (a > b); }).join(' ');
}

var iterate = (arr, op, cb) => {
  var next = function (el, cb) {
    if (el) {
      op(el, (err) => {
        if (err) {
          // log.error(el);
          // throw new Error(err);
          cb(err);
        }
        else {
          log.info(el);
          next(arr.shift(), cb);
        }
      });
    }
    else {
      if (cb) cb();
    }
  };

  next(arr.shift(), (err) => {
    if (err) {
      // log.error(err);
      // throw new Error(err);
    }
    else {
      log.info('done');
    }
    cb(err);
  });
}

var register = (incse, cse) => {

  var url;
  var ops = [
    (callback) => {
      var agent = getAgent(url);
      if (agent === mqttAgent) {
        agent.listen(url, incse.csi, handleRequestPrimitive);
      }
      callback(null, agent);
    },
    (agent, callback) => {
      // retrieve remoteCSE resource at IN_CSE
      var rqp = {
        rqi: m2m.util.createRequestID(),
        op: m2m.code.getOperation('Retrieve'),
        fr: cse.csi,
        to: '/' + incse.csi + '/' + incse.rn + '/' + cse.rn
      };
      agent.sendRQP(url, rqp, (err, rsp) => {
        if (!err) {
          log.info('[%s] %s %s... %s', logID, m2m.code.getOperation(rqp.op), rqp.to, m2m.code.getResponseStatusCode(+rsp.rsc));
        }
        callback(err, agent, rqp, rsp);
      });
    },
    (agent, rqp, rsp, callback) => {
      // create(or update) remoteCSE at IN_CSE 
      var csr = { poa: cse.poa };
      rqp.pc = {};
      rqp.rqi = m2m.util.createRequestID();
      rqp.pc[m2m.name.getShort('remoteCSE')] = csr;

      if (rsp.rsc === m2m.code.getResponseStatusCode('NOT_FOUND')) {
        csr.cb = '//' + spID + '/' + cseID;
        csr.cst = cse.cst;
        csr.csi = cse.csi;
        csr.rn = rqp.to.split('/').pop();
        rqp.op = m2m.code.getOperation('Create');
        rqp.to = ((path) => {
          var arr = rqp.to.split('/');
          return arr.splice(0, arr.length - 1).join('/');
        })(rqp.to);
        rqp.ty = m2m.code.getResourceType('remoteCSE');
      }
      else if (rsp.rsc === m2m.code.getResponseStatusCode('OK')) {
        rqp.op = m2m.code.getOperation('Update');
      }

      agent.sendRQP(url, rqp, (err, rsp) => {
        if (!err && rsp.rsc) {
          log.info('[%s] %s %s... %s', logID, m2m.code.getOperation(rqp.op), rqp.to, m2m.code.getResponseStatusCode(rsp.rsc));
        }
        callback(err, { success: !err && rsp.rsc});
      });
    }
  ];

  iterate(incse.poa.split(' '), (url_, cb) => {
    var parsed = require('url').parse(url_);
    if (!parsed.pathname) parsed.pathname = incse.csi;
    url = require('url').format(parsed);
    async.waterfall(ops, (err, res) => {
      if (err) {
        // console.error(err);
      }
      if (res && res.success) {
        log.debug('registration:', res);
      }
      cb((res ? res.success : null) || err);
    });
  },
    (err) => {
      // console.log(err); 
    });
}

var parsePOA = (poa) => {
  var parsed = url.parse(poa);
  var path = parsed.path.split('/');
  var cse = {
    csi: path[1],
    rn: path[2],
    cb: '//' + parsed.host + '/' + path[1],
    poa: poa
  };
  return cse;
}

/**
 * 
 */
var broadcastPrefix = '/oneM2M/cse/';
var broadcast = function (broker, cse) {
  var cseTopic = broadcastPrefix + cse.csi;

  var broadcast = () => {
    var msg = JSON.stringify(cse);
    log.info('[%s] broadcast <CSEBase> resource: ', logID, msg);
    broker.publish(cseTopic, msg, { retain: true });
  }

  broadcast();
  broker.subscribe(broadcastPrefix + '#', () => {
    broker.on('message', (top, msg) => {
      if (cseTopic === top && (!msg || msg.length < 1)) {
        broadcast();
      }
      else if (cseType !== m2m.code.getCseTypeID('IN_CSE')) {
        var tmp = JSON.parse(msg.toString());
        if (tmp && tmp.cst === m2m.code.getCseTypeID('IN_CSE') && tmp.rn === incse.rn) {
          incse = tmp;
          register(incse, cse);
        }
      }
    });
  })
}

/**
 * 
 */
exports.init = function (options) {

  // log.debug('----------------------------------------------');
  // log.debug(options);

  var isEmptyString = function (str) {
    return (!str || typeof str !== 'string' || str.length < 1);
  }

  //
  // set spID, cseID
  //
  spID = process.env.CSE_SP || options.sp; // [heroku]
  cseID = process.env.CSE_ID || options.id; // [heroku]
  let cseName = process.env.CSE_NAME || options.name; // [heroku]
  let inpoa = process.env.CSE_INPOA || options.inpoa; // [heroku]

  //
  // set cseType
  //
  if (options.type) {
    cseType = +options.type || m2m.code.getCseTypeID(options.type);
  }
  if (!inpoa) {
    if (cseType && cseType !== m2m.code.getCseTypeID('IN_CSE')) {
      throw new Error('[ERR] missing IN_CSE URL');
    }
    cseType = m2m.code.getCseTypeID('IN_CSE');
  }
  else if (!cseType) {
    cseType = m2m.code.getCseTypeID('MN_CSE');
  }

  //
  // set csePath
  //
  if (isEmptyString(cseName)) {
    throw new Error('[ERR] \'' + m2m.code.getCseTypeID(cseType) + ' name\' is not given');
  }
  csePath = util.format('%s', cseName);

  //
  // check infrastructure node information
  //
  if (inpoa) {
    incse = parsePOA(inpoa);
    if (!incse.rn) {
      // IN_CSE name is not given
      throw new Error('[ERR] \'IN_CSE name\' is not given: ' + inpoa);
    }
    if (cseName === incse.rn) {
      // MN_CSE(or ASN_CSE) name should be different from IN_CSE name
      throw new Error('[ERR] \'' + m2m.code.getCseTypeID(cseType) + ' name\' is identical to ' + cseName + ' the name of IN_CSE');
    }
  }

  //
  // show identity
  //
  logID = m2m.code.getCseTypeID(cseType);
  log.info('[%s] //%s/%s/%s', logID, spID, cseID, csePath);

  //
  // gather points of access
  //
  var poa = [];
  var broker;
  for (var protocol in options.bind) {
    var bind = options.bind[protocol];

    if (protocol === 'http') {
      bind.host = process.env.CSE_SP || bind.host; // [heroku]
      bind.port = process.env.PORT || bind.port; // [heroku]
      httpAgent = new (require('./binder')).HttpBinder(cseID);
      httpAgent.listen(bind.port, handleRequestPrimitive);
    }
    else if (protocol === 'mqtt') {
      let share = require('querystring').parse(options.labels).share;
      let url = process.env.CLOUDMQTT_URL; // [heroku]
      if (!url) url = require('url').format({
        slashes: true,
        protocol: 'mqtt:',
        hostname: bind.host,
        port: bind.port,
        auth: bind.username + ':' + bind.password
      });
      mqttAgent = new (require('./binder')).MqttBinder(options.id, {share: share});
      mqttAgent.listen(url, '+', handleRequestPrimitive);

      broker = mqttAgent.getClient((() => {
        let tmp = require('url').parse(url);
        if (tmp.auth) delete tmp.auth;
        return require('url').format(tmp);
      })());
    }
    else {
      throw new Error('[ERR] not supported \'point of access\' protocol: ' + protocol.name);
    }

    poa.push(protocol + "://" + bind.host + (bind.port ? ':' + bind.port : ''));
  }

  var rqp = {
    rqi: m2m.util.createRequestID(),
    op: m2m.code.getOperation('Retrieve'),
    fr: cseID,
    to: csePath,
    ty: m2m.code.getResourceType('CSEBase'),
    pc: {}
  };

  var obj = {
    csi: cseID,
    cst: cseType,
    srt: options.srt || getSrt(),
    poa: poa.join(' '),
    lbl: options.labels,
    cb: '//' + spID + '/' + cseID
  };

  rqp.pc[m2m.name.getShort('CSEBase')] = obj;

  //
  // finalize initialization 
  //      
  crud.operation(rqp, function (rsp) {

    if (rsp.rsc === m2m.code.getResponseStatusCode('OK')) {
      rqp.op = m2m.code.getOperation('Update');
    }
    else {
      rqp.op = m2m.code.getOperation('Create');
      rqp.to = '';
      obj.rn = cseName;
    }

    rqp.rqi = m2m.util.createRequestID();
    rqp.rcn = m2m.code.getResultContent('Attributes');
    crud.operation(rqp, function (rsp) {
      if (rsp.rsc === m2m.code.getResponseStatusCode('CREATED')) {
        log.info('[%s] created', logID);
      }
      else if (rsp.rsc === m2m.code.getResponseStatusCode('OK')) {
        log.info('[%s] updated', logID);
      }
      else {
        log.error('[%s] initialization failed', logID);
        throw new Error('initialization failure');
      }
      if (broker) {
        collectResource(rqp, csePath, (res) => {
          // announce CSEBase resource attributes
          broadcast(broker, res.cb);
          // register as a RemoteCSE on the IN_CSE
          if (cseType !== m2m.code.getCseTypeID('IN_CSE') && incse && res.cb) {
            register(incse, res.cb);
          }
        });
      }
    })
  });
}


exports.getAgent = getAgent;
exports.getCseID = () => { return cseID; };
exports.path2id = path2id;
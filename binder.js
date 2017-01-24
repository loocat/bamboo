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

var url = require('url');
var http = require('http');
var mqtt = require('mqtt');
var util = require('util');
var m2m = require('onem2m');

var log = (function () {
  try {
    return require('./logger').logger();
  }
  catch (e) {
    return {
      info: console.info,
      debug: console.log,
      warn: console.warn,
      error: console.error,
    };
  }
})();

var Binder = function () {
  var accessDefault = 'json';
  var getAccess = function (access) {
    return access || accessDefault;
  }

  var HttpBinder = function (myID) {

    //
    // make http parameters for request
    //
    var setup = function (rqp) {

      var options = {
        method: m2m.util.translateOperation(m2m.code.getOperation(rqp.op), 'http'),
        path: m2m.util.addr2path(rqp.to),
        headers: {
          'locale': 'ko',
          'X-M2M-RI': rqp.rqi || m2m.util.createRequestID(),
          'Accept': 'application/' + getAccess(rqp.ac || rqp.cty),
          'X-M2M-Origin': rqp.fr || myID,
          'Content-Type': m2m.util.mime(rqp)
        }
      };

      if (typeof rqp.ty === 'number') {
        options.headers['Content-Type'] += '; ty=' + rqp.ty;
        if (rqp.op === 'Create') {
          var arr = rqp.to.split('/');
          var target = arr.splice(0, arr.length - 1).join('/');
          if (target && target.length > 0) {
            options.headers['X-M2M-NM'] = target;
          }
        }
      }

      if (typeof rqp.pc !== 'undefined') {
        var length = (typeof rqp.pc === 'string') ? rqp.pc.length : JSON.stringify(rqp.pc).length;
        options.headers['Content-Length'] = length;
      }
      
      return options;
    }

    //
    // send a request primitive
    //
    this.sendRQP = (poa, rqp, callback) => {

      var header = setup(rqp); 

      var parsed = url.parse(poa);
      header.hostname = parsed.hostname;
      header.port = parsed.port;
      
      if (rqp.fc) {
        header.path += '?' + require('querystring').stringify(rqp.fc);
      }

      var req = http.request(header, function (res) {
        log.debug('[resp: ' + res.statusCode + ']');
        var data = '';
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
          data += chunk;
        });
        res.on('end', function () {
          if (callback) {
            callback(null, {
              rqi: res.headers['x-m2m-ri'],
              rsc: +(res.headers['x-m2m-rsc']),
              pc: data
            });
          }
        });
      });

      req.on('error', function (err) {
        log.error('problem with request: ' + err.message);
        if (callback) callback(err);
      });

      req.end(typeof rqp.pc === 'string' ? rqp.pc : JSON.stringify(rqp.pc));
    };

    var server;
    this.listen = function (port, handler) {
      if (server) {
        server.close();
      }
      server = http.createServer(function (httpReq, httpRsp) {
        httpRsp.sendResponse = function (to, rsp) {
          httpRsp.setHeader('X-M2M-RI', rsp.rqi);
          httpRsp.setHeader('X-M2M-RSC', rsp.rsc);
          httpRsp.setHeader('X-M2M-Origin', rsp.fr);
          httpRsp.statusCode = m2m.code.translateResponseStatusCodeToHttpStatusCode(rsp.rsc);
          var msg = rsp.pc;
          if (typeof msg !== 'string') {
            msg = JSON.stringify(msg);
          }
          httpRsp.end(msg);
        }

        var data = '';
        httpReq.on('data', (chunk) => { data += chunk; });
        httpReq.on('end', () => {
          m2m.util.processData(data, (msg) => {
            if (handler) {
              handler({rqp: makeRequestPrimitive(httpReq, msg), agent: httpRsp});
            }
          });
        });
      });
      server.listen(port, function () {
        log.info('oneM2M protocol HTTP binder running at ' + port + ' port');
      });
    };

    var determineOperation = function (method) {
      var ops = ['Create', 'Retrieve', 'Update', 'Delete', 'Notify'];
      for (var i in ops) {
        if (method === m2m.util.translateOperation(ops[i], 'http')) {
          return m2m.code.getOperation(ops[i]);
        }
      }
      // unknown http method: neither of POST, GET, PUT or DELETE
      return method;
    }
    
    var makeRequestPrimitive = function (httpReq, pc) {
      var rqp = {
        op: determineOperation(httpReq.method.toLowerCase()),
        fr: httpReq.headers['X-M2M-Origin'.toLowerCase()],
        to: m2m.util.path2addr(url.parse(httpReq.url).pathname),
        rqi: httpReq.headers['X-M2M-RI'.toLowerCase()],
        nmtype: httpReq.headers['nmtype'],
        pc: pc
      };

      var accept = httpReq.headers.accept;
      if (accept) {
        accept = accept.split('/')[1];
        if (accept !== '*') {
          rqp.ac = accept;
        }
      } 
      
      if (rqp.op === m2m.code.getOperation('Create') || rqp.op === m2m.code.getOperation('Update')) {
        var contentType = httpReq.headers['Content-Type'.toLowerCase()];
        if (contentType) {
          var arr = contentType.split(';');
          for (var ii = 0; ii < arr.length; ++ii) {
            var statement = arr[ii].trim();
            if (statement.lastIndexOf('=') > -1) {
              var tmp = statement.split('=');
              rqp[tmp[0].trim()] = tmp[1].trim(); 
            }
            else if (statement.lastIndexOf('application/vnd.onem2m-') === 0) {
              rqp.cty = statement.split('+')[1].trim();
            }
          }
          
          if (rqp.ty) {
            rqp.ty = Number.parseInt(rqp.ty);
          }
        }
      }
      
      // if (rqp.op === m2m.code.getOperation('Create')) {
      //   rqp.nm = pc.rn;
      //   if (pc.rn) delete pc.rn;
      //   if (rqp.name === null) {
      //     delete rqp.nm;
      //   }
      // }
      
      var query = url.parse(httpReq.url).query;
      if (query && query.length > 0) {
        rqp.fc = require('querystring').parse(url.parse(httpReq.url).query);
        
        [
          m2m.name.getShort('resultContent'),
          m2m.name.getShort('discoveryResultType'),
          m2m.name.getShort('stateTagSmaller'),
          m2m.name.getShort('stateTagBigger'),
          m2m.name.getShort('resourceType'),
          m2m.name.getShort('sizeAbove'),
          m2m.name.getShort('sizeBelow'),
          m2m.name.getShort('filterUsage'),
          m2m.name.getShort('limit'),
          m2m.name.getShort('filterOperation'),
          m2m.name.getShort('level'),
          m2m.name.getShort('offset')
        ].forEach((key) => {
          if (rqp.fc[key]) {
            if (Array.isArray(rqp.fc[key])) {
              for (var ii = 0; ii < rqp.fc[key].length; ++ii) {
                rqp.fc[key][ii] = +rqp.fc[key][ii]; 
              }
            }
            else {
              rqp.fc[key] = +rqp.fc[key];
            }
          }
        });

        if (rqp.fc) {
          Object.keys(rqp.fc).forEach((key) => {
            if (key === m2m.name.getShort('resourceType')) {
              return;
            }
            try {
              var ttt = m2m.name.getLong(key, 'PrimitiveParameters');
              rqp[key] = rqp.fc[key];
              delete rqp.fc[key];
            }
            catch (e) {
              // not a request primitive parameter
            }
          });

          if (Object.keys(rqp.fc).length < 1) {
            delete rqp.fc;
          }
        }
      }
      
      // log.debug(JSON.stringify(rqp, null, '  '));
      return rqp;
    }
  };

  var MqttBinder = function (myID, options) {
    if (!options) options = {};

    var self = this;

    if (!options.topicClass) options.topicClass = '/oneM2M';
    else options.topicClass = ('/' + options.topicClass).replace('//', '/');

    var clientOptions = function (r) {
      return {
        clientId: r.id,
        username: r.username,
        password: r.password,
        incomingStore: new mqtt.Store(),
        outgoingStore: new mqtt.Store(),
        // clean: false
      };
    }

    var qos = { subscribe: 1, publish: 1 };

    var getTopicSend = function (type, to, ext) {
      var patternTopicSend = options.topicClass + '/%s/%s/%s%s';
      return util.format(patternTopicSend, type, myID, to, (typeof ext === 'undefined') ? '' : '/' + ext);
    }

    var getTopicReceive = function (type, from, ext) {
      var patternTopicReceive = options.topicClass + '/%s/%s/%s%s';
      return util.format(patternTopicReceive, type, from, myID, (typeof ext === 'undefined') ? '/#' : '/' + ext);
    }
    
    var resolve = function (text) {
      let id, username, password;

      let parsed = text ? url.parse(text) : undefined;
      if (parsed) {
        if (parsed.path && parsed.host) {
          try {
            id = m2m.util.getCSERelativeAddress(m2m.util.path2addr(parsed.path), myID, parsed.host).split('/').pop();
          }
          catch (e) {
            console.log(parsed);
            console.log(text);
            console.log(m2m.util.path2addr(parsed.path));
            console.log(myID);
            throw e;
          }
        } 
        if (parsed.auth) {
          let arr = parsed.auth.split(':');
          if (arr.length === 2) {
            username = arr[0];
            password = arr[1];
          }
        }
      }

      let broker = _brokers[id || '+'];
      if (!broker) {
        if (parsed.host) {
          broker = url.format({
            protocol: parsed.protocol || 'mqtt:',
            host: parsed.host,
            slashes: true
          });
        }
        else {
          broker = _brokers['+'];
        }
      }
      return {
        broker: broker,
        id: id,
        username: username,
        password: password
      };
    };

    this.getTopicSend = getTopicSend;
    this.getTopicReceive = getTopicReceive;

    var setup = (rqp) => {

      if (!rqp.fr) rqp.fr = myID;
      if (!rqp.rqi) rqp.rqi = m2m.util.createRequestID();

      if (typeof rqp.ty === 'number') {
        if (rqp.op === 'Create' || rqp.op === 'Update') {
          if (typeof rqp.pc === 'undefined') {
            log.error('empty primitive content for "' + rqp.op + '" operation');
            throw rqp.op;
          }
          if (typeof rqp.pc === 'object') {
            if (!rqp.pc[m2m.code.getResourceType(rqp.ty)]) {
              log.error('empty resource object for "' + rqp.op + '" operation');
              throw rqp.op;
            }
          }
        }

        if (rqp.op === 'Create') {
          var index = path.lastIndexOf('/');
          var target = path.slice(index + 1);
          var parent = path.slice(0, index);

          rqp.to = parent;
          if (target && target.length > 0) {
            rqp.pc[m2m.code.getResourceType(rqp.ty)].rn = target;
          }
        }
      }

      if (rqp.fc && ('rcn' in rqp.fc)) {
        rqp.rcn = rqp.fc.rcn;
        delete rqp.fc.rcn;
      }

      return rqp;
    }
    
    var topicRE = new RegExp('^' + options.topicClass + '/(req|resp)/*');
    var _incoming = {}; // incoming requests
    var _outgoing = {}; // outgoing requests
    var _clients = {}; // maps broker to client
    var _brokers = {}; // maps id to broker 

    var getIncoming = function (src) {
      if (!_incoming[src]) {
        _incoming[src] = {};
      }
      return _incoming[src];
    };
    
    var getOutgoing = function (dst) {
      if (!_outgoing[dst]) {
        _outgoing[dst] = {};
      }
      return _outgoing[dst];
    };

    this.sendRQP = (poa, rqp, callback) => {

      let cty = getAccess(rqp.cty || rqp.ac);
      if (rqp.cty) delete rqp.cty;

      rqp = setup(rqp);
      var msg = JSON.stringify(rqp);

      var resolved = resolve(poa);
      if (!poa) {
        resolved.id = myID;
      }
      if (!resolved.id) {
        resolved.id = rqp.to.split('/')[1];
      }

      var topic = getTopicSend('req', resolved.id, cty);
      var client = getClient(resolved);

      if (client) client.publish(topic, msg, function () {
        // log.info('[%s] send %s', rqp.rqi, topic);
        var outgoing = getOutgoing(resolved.id);
        if (!rqp || typeof rqp === 'undefiend') return;
        if (outgoing[rqp.rqi]) {
          log.error('[DUPLICATED] RQI(%s) to CSEID(%s)', rqp.rqi, resolved.id);
          throw new Error();
        }
        outgoing[rqp.rqi] = {
          rqp: rqp,
          callback: callback,
          timer: setTimeout(function (rqi) {
            if (outgoing[rqi]) {
              // log.warn('[%s] timeout', rqi);
              if (outgoing[rqi].callback) {
                outgoing[rqi].callback({ err: 'TIMEOUT', rqp: outgoing[rqi].rqp});
              }
              delete outgoing[rqi];
            }
          }, 20000, rqp.rqi)
        };
      });
    };

    var getRequestSource = function (topic) {
      return topic.replace(/^.*\/req\/|^.*\/resp\//, '').split('/')[0];
    }

    var getContentType = function (topic) {
      return topic.replace(/^.*\/req\/|^.*\/resp\//, '').split('/')[2];
    }

    this.drop = function (msg) {
      var incoming = getIncoming(getRequestSource(msg.topic));
      if (msg.rqp && msg.rqp.rqi && incoming[msg.rqp.rqi]) {
        delete incoming[msg.rqp.rqi];
      }
    }

    this.sendResponse = function (url, options, pc) {

      var resolved = resolve(url);
      resolved.id = resolved.id || url;
      var incoming = getIncoming(resolved.id);
      if (!incoming[options.rqi]) {
        log.error('[UNKNOWN] RQI %s', options.rqi);
        return;
      }

      let rqp = incoming[options.rqi].rqp;
      var rsp = {};
      for (var i in options) {
        rsp[i] = options[i];
      }
      if (pc) rsp.pc = pc;
      var message = JSON.stringify(rsp);
      var topic = getTopicSend('resp', resolved.id, rqp.ac || rqp.cty);
      log.debug('[%s] %s', rsp.rqi, topic);
      _clients[resolved.broker].publish(topic, message);

      delete incoming[options.rqi];
    }
    
    var getClient = function (r) {
      var client = _clients[r.broker]; 
      if (!client) {
        var topic = (options.share ? '$share:' + myID + ':' : '') + getTopicReceive('+', r.target || '+');        
        client = mqtt.connect(r.broker, clientOptions(r));
        client.on('connect', () => {
          client.subscribe(topic, {qos: qos.subscribe}, (err, granted) => {
            if (err) {
              console.error(err);
              throw new Error();
            }
            log.debug('listening ', r.broker);
            log.debug(JSON.stringify(granted, null, '  '));
          });
        });
        _clients[r.broker] = client;
        _brokers[r.id || '+'] = r.broker;
      }
      return _clients[r.broker];
    };

    var listen = function (poa, target, handler) {
      var resolved = resolve(poa);
      resolved.target = target;
      var client = _clients[resolved.broker];
      if (client) {
        return; // exist
      } 
      client = getClient(resolved); 
      client.on('message', function (topic, message) {
        if (topicRE.test(topic)) m2m.util.processData(message, (msg) => {
          // console.log(message.toString());
          // console.log(JSON.stringify(JSON.parse(message.toString()), null, ' '));
          // console.log(JSON.stringify(msg, null, ' '));
          if (!msg) {
            log.error('missing primitive:', message.toString());
          }
          else if (!msg.rqi) {
            log.error('missing \'request id\':', msg);
          }
          else if (!msg.fr) {
            log.error('missing \'from\':', msg);
          }
          else if (!msg.to) {
            log.error('missing \'to\':', msg);
          }
          else if (msg.op) {
            // request primitive
            msg.to = m2m.util.path2addr(msg.to);
            msg.op = +msg.op; 
            var incoming = getIncoming(getRequestSource(topic));
            if (incoming[msg.rqi]) {
              log.warn('[DUP] %s', msg.rqi);
            }
            else if (handler) {
              if (!msg.cty) {
                msg.cty = getContentType(topic);
              }
              incoming[msg.rqi] = {
                topic: topic,
                rqp: msg,
                agent: self
              };
              handler(incoming[msg.rqi]);
            }
          }
          else if (msg.rsc) {
            // response primitive
            var outgoing = getOutgoing(getRequestSource(topic));
            if (outgoing[msg.rqi]) {
              msg.rsc = +msg.rsc; 
              var request = outgoing[msg.rqi];
              if (request) {
                if (request.callback) request.callback(null, msg);
                if (request.timer) clearTimeout(request.timer);
                delete outgoing[msg.rqi];
              }
            }
          }
          else {
            log.error('invalid message:', msg);
          }
        });
      });
    };
    
    this.getClient = (broker) => { return _clients[broker]; };
    this.listen = listen;
    
    var diag = () => {
      var count = (obj) => {
        return (obj ? Object.keys(obj).length : 0);
      };
      var first = true;
      for (var ii in _incoming) {
        var ttt = [ count(_incoming[ii]), count(_outgoing[ii]) ];
        if (ttt[0] > 0 || ttt[1] > 0) {
          if (first) {
            console.log('\n' + options.topicClass + '________________________________________________________________________');
            first = false;
          }
          console.log(ii, ' incoming', ttt[0]);
          console.log(ii, ' outgoing', ttt[1]);
          console.log('-------------------------------------------------------------------------------');
        }
      }
    }
    // setInterval(diag, 3000);

  }

  var binder = this;
  binder.HttpBinder = HttpBinder;
  binder.MqttBinder = MqttBinder;
}

module.exports = new Binder(); 
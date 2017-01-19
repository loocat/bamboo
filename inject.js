
var tree = process.argv[2] || './tree.json';
var m2m = require('onem2m');
var name = m2m.name;
var async = require('async');
var xml2js = require('xml2js');
var moment = require('moment');
var uuid = require('uuid');
var util = require('util');

var agent;
var cse;
var clean = false;
var logID;
var proto = 'mqtt:';
// var proto = 'http:';

var log = m2m.log;

var setup = function (node) {
  cse = node;
  var binder = require('./binder'); 
  if (proto === 'mqtt:') {
    agent = new binder.MqttBinder('inject');
    agent.listen(getPointOfAccess(proto), cse.csi);
  }
  else {
    agent = new binder.HttpBinder(cse.csi);
  }
  clean = require('./conf').clean;
  logID = m2m.code.getCseTypeID(3);
}

var show = (path, node, rsc) => {
  console.log(util.format('[%s] %s ... %s',
    m2m.name.getShort(m2m.code.getResourceType(node.ty)),
    path + '/' + node.rn,
    m2m.code.getResponseStatusCode(rsc)
  ));
};

/**
 * 
 */
var handleMqttMessage = function (topic, message, agent) {
  
  if (!topic || topic.length < 1 || !message || message.length < 1) return;

  var topic_arr = topic.split("/");

  if (topic_arr[1] === 'oneM2M' && (topic_arr[2] === 'req' || topic_arr[2] === 'resp')) {
    var parser = new xml2js.Parser({ explicitArray: true });
    parser.parseString(message, function (err, result) {
      var msg = err ? m2m.util.parseJson(message) : result;
      if (msg) {
        console.log(JSON.stringify(msg, null, ' '));
        if (msg.rqp) {
          // console.log(JSON.stringify(msg.req, null, ' '));
          // handleRequestPrimitive(req, agent);
        }
        else if (msg.rsp) {
          if (handlers[msg.rsp.rqi]) {
            var handler = handlers[msg.rsp.rqi];
            msg.rsp.rsc = +msg.rsp.rsc; 
            handler(null, msg.rsp);
            delete handlers[msg.rsp.rqi];
          }
        }
        else {
          console.error('[%s] missing json object: \'req\'', logID);
        }
      }
    });
  }
  else {
    console.error('[%s] topic is not supported: %s', logID, topic);
  }
};

var request = function (rqp, callback) {
  // console.dir(rqp);
  agent.sendRQP(getPointOfAccess(proto), rqp, callback);
}

var retrieve = function (path, callback) {
  if (agent) {
    var rqp = {
      op: m2m.code.getOperation('Retrieve'),
      fr: 'inject',
      to: path
    };
    request(rqp, callback);
  }
}

var discovery = function (path, filterCrieteria, callback) {
  if (agent) {
    var rqp = {
      op: m2m.code.getOperation('Retrieve'),
      fr: 'inject',
      to: path,
      fc: filterCrieteria
    }
    request(rqp, callback);
  }
}

var attributesIgnore = [
  'ty',
  'ri',
  'rn',
  'ct',
  'lt',
  'cr',
  'toc',
  'cs',
  'aei',
  'cni',
  'cbs',
  'pi',
  'st'
] 

var attribute = function (ref) {
  var tmp = {};
  for (var ii in ref) {
    if (!Array.isArray(ref[ii]) && attributesIgnore.indexOf(ii) < 0) {
      tmp[ii] = (typeof ref[ii] === 'object') ? JSON.stringify(ref[ii]) : ref[ii];
    } 
  }
  return tmp; 
}

var create = function (org, path, ty, ref, callback) {
  var arr = path.split('/');
  var child = arr.pop();
  var parent = arr.join('/');
  var rqp = {
    op: m2m.code.getOperation('Create'),
    fr: org,
    to: parent,
    ty: ty,
    nm: (child && child.length > 0) ? child : undefined,
    pc: m2m.util.wrapMessage(m2m.code.getResourceType(ty), ref)
  }
  request(rqp, callback);
}

var isAE = function (node) {
  return (node.ty === m2m.code.getResourceType('AE'))
}

var specialContainers = ['inf', 'exe', 'dat', 'sts'];

var isSpecialContainer = function (node) {
  if (node.ty === m2m.code.getResourceType('container')) {
    if (specialContainers.indexOf(node.rn) !== -1) {
      return true;
    }
  }
  return false;
}

var isCSE = function (node) {
  return (m2m.code.getResourceType('CSEBase') === node.ty || m2m.code.getResourceType('remoteCSE') === node.ty); 
}

var isTerminal = function (node) {
  if (!isSpecialContainer(node)) {
    if (!node.cnt || !Array.isArray(node.cnt)) {
      return (!isCSE(node));
    }
    for (var jj in node.cnt) {
      if (isSpecialContainer(node.cnt[jj])) {
        return true;
      }
    }
  }
  return false;
}

var getPointOfAccess = function (preferred, id) {
  var url = require('url');
  var arr = cse.poa.split(' ');
  var poa;
  for (var ii = 0; ii < arr.length; ++ii) {
    if (url.parse(arr[ii]).protocol === preferred) {
      poa = arr[ii];
    }
  }
  if (!poa) poa = arr[0];
  if (id) {
    poa = url.parse(poa);
    // poa.pathname = '/' + id; // CSEBase relative path
    poa.pathname = null;
    poa = url.format(poa);
  }
  return poa;
}

var subscribe = function (path, info) {
  var ty = m2m.code.getResourceType('subscription');
  discovery(path, {
    rcn: m2m.code.getResultContent('Child resources'),
    cr: info.aei,
    ty: ty
  }, (err, res) => {
    if (err) {
      console.error(err);
      throw new Error();
      // return;
    }
    if (res.rsc === m2m.code.getResponseStatusCode('NOT_FOUND')) {
      // console.error('[%s] %s', m2m.getResponseStatusCode(res.rsc), path);
      // throw new Error();
      return;
    }
    if (res.rsc === m2m.code.getResponseStatusCode('BAD_REQUEST')) {
      console.error('[%s] %s', m2m.code.getResponseStatusCode(res.rsc), path);
      throw new Error();
    }
    // console.log(info.aei + ' ----> ' + path);
    // console.log(JSON.stringify(res, null, '  '));
    if (res.rsc !== m2m.code.getResponseStatusCode('OK')) {
      throw new Error();
    }
    if (typeof res.pc === 'string') {
      res.pc = m2m.util.parseJson(res.pc);
    }
    var cnt = res.pc.cnt;
    var sub = undefined;
    var creator = info.aei;
    if (cnt && cnt.sub && Array.isArray(cnt.sub)) {
      for (var ii = 0; ii < cnt.sub.length && !sub; ++ii) {
        if (cnt.sub[ii].cr === creator) { sub = cnt.sub[ii]; }
      }
    }
    
    if (sub) {
      show(path, sub, res.rsc);
    }
    else {
      create(creator, path + '/', ty, {
        enc: {
          om: {
            opr: 1
          },
          atr: info.atr
        },
        nu: getPointOfAccess('mqtt:', info.aei)
      },
      (err, res) => {
        if (err) {
          console.error(err);
          throw new Error();
          // return;
        }
        if (res.rsc === m2m.code.getResponseStatusCode('CREATED')) {
          if (typeof res.pc === 'string') {
            res.pc = m2m.util.parseJson(res.pc);
          }
        }
        show(path, res.pc.sub, res.rsc);
      });
    }
  });
}

var appendSubscription = function (target, aei, atr) {
  if (!sss) sss = {};
  if (!sss[target]) sss[target] = [];
  sss[target].push({aei: aei, atr: atr});
}

var dive = function (node, path, aei, cb) {

  if (node.ty === 4 || node.ty === 23) {
    return;
  }

  if (clean) {
    var ttt = ['inf', 'exe', 'dat', 'sts'];
    if (isAE(node)) {
      ttt.splice(ttt.indexOf('dat'), 1);
    }
    else if (!isTerminal(node)) {
      ttt = [];
    }

    var makeContainer = function (name) {
      var cnt = {
        ty: m2m.code.getResourceType('container'),
        rn: name
      };
      
      if (name === 'exe') {
        cnt.sub = [ path + '/' + name ];
      }
      else if (name === 'inf') {
        var arr = path.split('/'); 

        var con = {
          id: uuid.v1(),
          type: '',
          model: '',
          manufacturer: 'SK Holdings C&C',
          location: '',
          coordinate: {map: '', x: '', y:''},
          gatewayName: arr[3],
          installedDate: moment().format("YYYY-MM-DD"),
          description: '',
          manager: ''
        };
        
        if (arr.legnth < 3) {
          // gateway
          con.aeId = aei;
          con.notificationURI = '';
        }
        else {
          // device
          con.deviceName = arr[4];
        }
        
        cnt.cin = [{
          rn: '0',
          ty: 4,
          con: JSON.stringify(con)
        }];
      }      
      else if (name === 'sts') {
        var arr = (path + '/sts').split('/').splice(2);

        var con = {
          nm: arr.join('/'),
          st: moment().format('YYYYMMDDTHHmmss'),
          sc: 3,
          sm: 'initial status'
        };
        
        cnt.cin = [{
          rn: '0',
          ty: 4,
          con: JSON.stringify(con)
        }];
      }      
      
      return cnt;
    }

    if (ttt.length > 0) {
      if (!node.cnt) node.cnt = [];
      for (var ii = 0; ii < ttt.length; ++ii) {
        var jj = 0;
        for (; jj < node.cnt.length; ++jj) {
          if (node.cnt[jj] && node.cnt[jj].rn === ttt[ii]) {
            node.cnt[jj] = undefined;
            break;
          }
        }
        if (jj < node.cnt.length) {
          node.cnt.splice(jj, 1);
        }
        node.cnt.push(makeContainer(ttt[ii]));
      }
    }
  }

  for (var ii in node) {
    if (ii !== 'sub' && Array.isArray(node[ii])) {
      node[ii].forEach(function (ee) {
        if (typeof ee === 'object') {
          trace(ee, path, aei, cb);
        }
      });
    }
  }

  if (!!node.sub) {
    if (!Array.isArray(node.sub)) {
      node.sub = [node.sub];
    }  
    node.sub.forEach(function (ee) {
      var target;
      var atr = { ty: 4 };
      if (Array.isArray(ee)) {
        target = ee.shift();
        var tmp = ee.shift();
        if (tmp) for (var kk in tmp) {
          atr[kk] = tmp[kk];
        }
      }
      else if (typeof ee === 'string') {
        target = ee;
      }
      
      if (target) {
        if (!target.match(/^\//)) {
          target = '/' + tree.csi + '/' + target;
        }
        appendSubscription(target, aei, atr);
      }
    });
  }
  
  if (node.rn && cb) {
    cb(node.rn === path.split('/')[1] ? undefined : path);
  }
};

var trace = function (node, parent, aei, cb) {
  if (!parent) {
    parent = '';
  }
  
  if (!agent && node.ty === m2m.code.getResourceType('CSEBase')) {
    setup(node);
  }

  // if (node.rn) {
  if (node.ty) {
    // var path = parent + '/' + (!!node.rn ? node.rn : m2m.util.createResourceID(node.ty));
    // var path = parent + '/' + (!!node.rn ? node.rn : '');
    var path = ((parent.length > 0) ? parent + '/' : '') + (!!node.rn ? node.rn : '');
    var retry = 1;
    var ref = attribute(node);

    var retrieveAndCreate = () => {
      retrieve(path, afterRetrieve);
    }

    var afterRetrieve = function (err, res) {
      if (err) {
        console.error(err);
        throw new Error();
        // return;
      }
      // console.log('RETRIEVE', m2m.code.getResponseStatusCode(res.rsc), path);
      if (!isNaN(res.rsc)) {
        // console.log(res.rsc);
        if (res.rsc !== m2m.code.getResponseStatusCode('NOT_FOUND')) {
          if (typeof res.pc === 'string') {
            res.pc = m2m.util.parseJson(res.pc);
          }
          show(parent, node, res.rsc);
          dive(node, path, (isAE(node) && res.pc.ae) ? res.pc.ae.aei : aei, cb);
        }
        else {
          // console.log(path + ' --> not found');
          // console.log(ref);
          create('inject', path, node.ty, ref, afterCreate);
        }
      }
      else {
        console.log(res);
        throw new Error(res);
      }
    };

    var afterCreate = (err, res) => {
      if (err) {
        console.error(JSON.stringify(err, null, ' '));
        throw new Error(err);
      }
      var goon = true;
      show(parent, node, res.rsc);
      if (res.rsc === m2m.code.getResponseStatusCode('CREATED')) {
      }
      else if (res.rsc === m2m.getResponseStatusCode('ALREADY_EXISTS')) {
      }
      else {
        if (--retry > 0) {
          console.log('RETRY-----------------------', retry);
          create('inject', path, node.ty, ref, afterCreate);
          // setTimeout(retrieveAndCreate, Math.random() * 4000 + 1000);
        }
        else {
          console.error(res);
          throw new Error(res);
        }
        goon = false;
      }

      if (goon) {
        if (typeof res.pc === 'string') {
          res.pc = m2m.util.parseJson(res.pc);
        }
        dive(node, path, (isAE(node) && res.pc.ae) ? res.pc.ae.aei : aei, cb);
      }
    };    

    retrieveAndCreate();
  }
}

var sss;

//
// finalization: create subscriptions
//
var finalize = function (target) {
  // console.log('FIN', target);
  if (sss) {
    if (!target) {
      target = Object.keys(sss);
    }
    if (!Array.isArray(target)) {
      target = [target];
    }
    target.forEach(function (path) {
      if (sss[path]) {
        // console.log('finalize...', path, sss[path]);
        sss[path].forEach(function (info) {
          subscribe(path, info);
        });
      }
    });
  }
}

var tree = m2m.util.toShortName(require(tree).CSEBase);
var xxx = trace(tree, '/' + tree.csi, undefined, finalize);
// var aaa = m2m.util.toShortName(require(tree).CSEBase);
// trace(aaa);
// console.log(JSON.stringify(aaa, null, ' '));

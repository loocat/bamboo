var db = require('./db').getDB();
var m2m = require('onem2m');
var moment = require('moment');

exports.operation = function (req, callback) {
  if (req.op === m2m.code.getOperation('Create')) {
    Create(req, function (rsp) {
      if (callback) callback(rsp);
    });
  } else if (req.op === m2m.code.getOperation('Retrieve')) {
    RetrieveOrDiscovery(req, function (rsp) {
      if (callback) callback(rsp);
    });
  } else if (req.op == m2m.code.getOperation('Update')) {
    Update(req, function (rsp) {
      if (callback) callback(rsp);
    });
  } else if (req.op == m2m.code.getOperation('Delete')) {
    Delete(req, function (rsp) {
      if (callback) callback(rsp);
    });
  } else {
    log.debug("Operation is invalid");
    var rsp = {};
    rsp.to = req.fr;
    rsp.fr = req.to;
    rsp.rqi = req.rqi;
    rsp.ot = req.ot;
    rsp.rset = req.rset;
    rsp.ec = req.ec;
    rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
    if (callback) callback(rsp);
  }
}

exports.discovery = (db.discovery ? Discovery : undefined);

var getNow = function () {
  return moment().format("YYYYMMDDTHHmmss");
}

var updateParent = function (parent, child, delta) {

  if (!parent) return;

  var row = {
    path: parent.path,
    lastmodifiedtime: child.lastmodifiedtime
  };

  if (parent.resourcetype === m2m.code.getResourceType('container') ||
    parent.resourcetype === m2m.code.getResourceType('contentInstance') ||
    parent.resourcetype === m2m.code.getResourceType('delivery') ||
    parent.resourcetype === m2m.code.getResourceType('request')) {
    // increase parent stateTag by 1
    row.statetag = parent.statetag + 1;
  }

  if (typeof delta === 'number' && parent.resourcetype === m2m.code.getResourceType('container')) {
    if (delta > 0) {
      // increase currentNumberOfInstances by 1
      row.currentnrofinstances = parent.currentnrofinstances + 1;
    }
    else if (delta < 0) {
      // decrease currentNumberOfInstances by 1
      row.currentnrofinstances = parent.currentnrofinstances - 1;
    }
    // update currentByteSize
    row.currentbytesize = parent.currentbytesize + delta;
  }

  db.update(row);
}

function Create(req, callback) {
  db.retrieve(req.to, undefined, function (err, rows) {
    if (err || (req.to !== '' && rows.length < 1)) {
      var rsp = {
        rsc: m2m.code.getResponseStatusCode('BAD_REQUEST'),
        fr: req.to,
        to: req.fr,
        rqi: req.rqi,
        ot: req.ot,
        rset: req.rset,
        ec: req.ec
      };
      if (callback) callback(rsp);
    }
    else {
      createChild((rows && rows.length > 0) ? rows[0] : undefined);
    }
  });

  var createChild = function (parent) {
    var resourceID = m2m.util.createResourceID(req.ty);
    var value = req.pc[m2m.name.getShort(m2m.code.getResourceType(req.ty))];
    var resourceName = value.rn || resourceID;

    var lv = {};
    if (parent) lv.parentid = parent.resourceid;
    lv.parentpath = req.to;
    lv.path = (req.to + '/' + resourceName).replace('//', '/').replace(/^\//, '');
    lv.creator = req.fr;
    lv.resourcetype = req.ty;
    lv.resourcename = resourceName;
    lv.resourceid = resourceID;
    if (req.ty == m2m.code.getResourceType('AE')) {
      lv.aeid = resourceID;
    }
    // CO, CI, SS 경우
    if (req.ty == 3 || req.ty == 4 || req.ty == 23) {
      lv.statetag = 0;
    }
    for (var i in value) {
      var fld = m2m.name.getLong(i, attributeDictionary).toLowerCase().replace(/-+/g, '');
      lv[fld] = trans(i, value[i]);
    }

    // set contentInstance:contentSize, contentInfo
    if (lv.content) {
      if (typeof lv.content !== 'string') {
        lv.content = JSON.stringify(lv.content);
      }
      lv.contentsize = lv.content.length;
      if (!lv.contentinfo) {
        lv.contentinfo = 'text/plain:0';
      }
    }

    var now = getNow();
    lv.creationtime = now;
    lv.lastmodifiedtime = now;

    db.create(lv, function (err, res) {
      var rsp = {};
      rsp.to = req.fr;
      rsp.fr = lv.path;
      rsp.rqi = req.rqi;
      rsp.ot = req.ot;
      rsp.rset = req.rset;
      rsp.ec = req.ec;

      if (err) {
        // for MySQL
        if (db.dbms === 'MySQL' && res.code === 'ER_DUP_ENTRY') {
          rsp.rsc = m2m.code.getResponseStatusCode('ALREADY_EXISTS');
        }
        // for MongoDB
        else if (db.dbms === 'MongoDB' && err.code === 11000) {
          rsp.rsc = m2m.code.getResponseStatusCode('ALREADY_EXISTS');
        }
        // for HANA
        else if (db.dbms === 'HANA' && err.code === 301) {
          rsp.rsc = m2m.code.getResponseStatusCode('ALREADY_EXISTS');
        }
        // Unknown database
        else {
          rsp.rsc = m2m.code.getResponseStatusCode('INTERNAL_SERVER_ERROR');
        }
      }
      else {
        rsp.rsc = m2m.code.getResponseStatusCode('CREATED');
        updateParent(parent, lv, lv.contentsize);
      }
      if (callback) callback(rsp);
    });
  }
}

var privateFields = [
  'path',
  'parentpath',
  'm2mextid',
  'triggerrecipientid',
  'groupid'
];

var isPublicField = function (name) {
  for (var ii in privateFields) {
    if (privateFields[ii] === name) return false;
  }
  return true;
}

var attributeDictionary = ['ResourceAttributes', 'ComplexDataTypesMembers'];

var trans = (function () {
  var num = [
    'cseType',
    'resourceType',
    'triggerRecipientID',
    'stateTag',
    'maxNrOfInstances',
    'maxByteSize',
    'maxInstanceAge',
    'currentNrOfInstances',
    'currentByteSize',
    'contentSize',
    'pendingNotification',
    'notificationContentType',
    'notificationEventCat',
    'expirationCounter',
    'preSubscriptionNotify',
    'memberType',
    'maxNrOfMembers'
  ];

  var obj = [
    //'accessControlPolicyIDs',
    'objectIDs',
    'applicableCredIDs',
    'allowedApp-IDs',
    'allowedAEs',
    'eventNotificationCriteria',
    'batchNotify',
    'rateLimit',
    'privileges',
    'selfPrivileges'
  ];

  var pool = {};
  for (var ii in num) {
    pool[m2m.name.getShort(num[ii])] = function (val) { return (typeof val === 'number' ? val : Number.parseInt(val)); };
  }
  for (var ii in obj) {
    pool[m2m.name.getShort(obj[ii])] = function (val) { return (typeof val !== 'string' ? val : JSON.parse(val)); };
  }

  return function (name, value) {
    if (pool[name]) {
      return pool[name](value);
    }
    if (typeof value === 'object' && Object.keys(value).length < 1) {
      return null;
    }
    return value;
  };

})();

var rowToResource = function (row) {
  var res = {};
  for (var ii in row) {
    if (typeof row[ii] !== 'undefined' && row[ii] !== null) {
      if (isPublicField(ii)) {
        try {
          var shortName = m2m.name.flatToShort(ii.toLowerCase(), attributeDictionary);
          res[shortName] = trans(shortName, row[ii]);
        }
        catch (e) {
          // ignore
        }
      }
      else if (ii === 'path') {
        res[ii] = row[ii];
      }
    }
  }
  return m2m.util.wrapMessage(m2m.code.getResourceType(+res.ty), res);
}

function RetrieveOrDiscovery (rqp, callback) {
  if (db.discovery && rqp.fc && rqp.fc.fu === m2m.code.getFilterUsage('Discovery Criteria')) {
    Discovery(rqp, callback);
  }
  else {
    Retrieve(rqp, callback);
  }
}

function Discovery(rqp, callback) {
  var rsp = {
    to: rqp.fr,
    fr: rqp.to,
    rqi: rqp.rqi
  };
  var drt = (rqp.drt || m2m.code.getDiscResType('structured'));
  var order;
  if (rqp.dst) {
    order = (rqp.dst === m2m.code.getSortType('Descending')) ? 'DESC' : 'ASC';
  }
  db.discovery(
    rqp.to,
    rqp.fc,
    rqp.rcn || m2m.code.getResultContent('Child resource references'),
    drt,
    order,
    (err, rows) => {
      if (err) {
        rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
      }
      else if (!rows || rows.length < 1) {
        rsp.rsc = m2m.code.getResponseStatusCode('NOT_FOUND');
      }
      else {
        rsp.rsc = m2m.code.getResponseStatusCode('OK');
        rsp.pc = [];
        if (!Array.isArray(rows)) {
          rows = [rows];
        }
        var field = (drt === m2m.code.getDiscResType('structured') ? 'path' : 'resourceid');
        rows.forEach((row) => { rsp.pc.push(row[field]); });
      }
      if (callback) callback(rsp);
    }
  );
}

function Retrieve(req, callback) {
  var lv = {};
  lv.path = req.to;
  db.retrieve(lv.path, req.fc, function (err, rows) {
    var rsp = {};
    rsp.to = req.fr;
    rsp.fr = req.to;
    rsp.rqi = req.rqi;
    rsp.ot = req.ot;
    rsp.rset = req.rset;
    rsp.ec = req.ec;
    if (err) {
      //rsp.rsc = '1000';
      rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
    }
    else if (!rows || rows.length < 1) {
      rsp.rsc = m2m.code.getResponseStatusCode('NOT_FOUND');
    }
    else {
      rsp.rsc = m2m.code.getResponseStatusCode('OK');
      rsp.pc = [];
      if (!Array.isArray(rows)) {
        rows = [rows];
      }
      rows.forEach((row) => { rsp.pc.push(rowToResource(row)); });
    }
    if (callback) callback(rsp);
  });
}

function Update(req, callback) {
  var lv = {};
  lv.path = req.to;
  for (var key in req.pc) {
    var value = req.pc[m2m.name.getLong(key, 'ResourceTypes')] || req.pc[m2m.name.getShort(key, 'ResourceTypes')];
    for (var i in value) {
      var fld = m2m.name.getLong(i, attributeDictionary).toLowerCase().replace(/-+/g, '');
      lv[fld] = trans(i, value[i]);
    }
  }

  // set contentInstance:contentSize
  if (lv.content) {
    lv.contentsize = lv.content.length;
  }

  lv.lastmodifiedtime = getNow();

  db.update(lv, req.fc, function (err, res) {
    var rsp = {};
    rsp.to = req.fr;
    rsp.fr = req.to;
    rsp.rqi = req.rqi;
    rsp.ot = req.ot;
    rsp.rset = req.rset;
    rsp.ec = req.ec;
    if (err) {
      if (res.code === 'ER_PARSE_ERR') {
      }
      //rsp.rsc = '1000';
      rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
    }
    else {
      rsp.rsc = m2m.code.getResponseStatusCode('OK');
      var parentPath = req.to.slice(0, req.to.lastIndexOf('/'));
      if (parentPath.lastIndexOf('/') > 0) {
        db.retrieve(parentPath, undefined, function (err, rows) {
          if (!err) updateParent(rows[0], lv);
        });
      }
    }
    if (callback) callback(rsp);
  });
}

function Delete(req, callback) {
  var deleteResource = function (parent, delta) {
    db.delete(req.to, req.fc, function (err, res) {
      var rsp = {};
      rsp.to = req.fr;
      rsp.fr = req.to;
      rsp.rqi = req.rqi;
      rsp.ot = req.ot;
      rsp.rset = req.rset;
      rsp.ec = req.ec;

      // for MySQL
      if (db.dbms === 'MySQL') {
        if (err) {
          rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
        }
        else if (res && res.affectedRows === 1) {
          rsp.rsc = m2m.code.getResponseStatusCode('OK');
          updateParent(parent, { path: req.to, lastmodifiedtime: getNow() }, delta);
        }
        else if (res && res.affectedRows === 0) {
          rsp.rsc = m2m.code.getResponseStatusCode('NOT_FOUND');
        }
        else {
          rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
        }
      }

      // for MongoDB
      else if (db.dbms === 'MongoDB') {
        if (err || (res && res.result.ok !== 1)) {
          rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
        }
        else if (res && res.result.ok === 1 && res.result.n > 0) {
          rsp.rsc = m2m.code.getResponseStatusCode('OK');
          updateParent(parent, { path: req.to, lastmodifiedtime: getNow() }, delta);
        }
        else if (res && res.result.ok === 1 && res.result.n === 0) {
          rsp.rsc = m2m.code.getResponseStatusCode('NOT_FOUND');
        }
        else {
          rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
        }
      }
      
      // for HANA
      else if (db.dbms === 'HANA') {
        if (err) {
          rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
        }
        else if (res && res === 1) {
          rsp.rsc = m2m.code.getResponseStatusCode('OK');
          updateParent(parent, { path: req.to, lastmodifiedtime: getNow() }, delta);
        }
        else if (res && res === 0) {
          rsp.rsc = m2m.code.getResponseStatusCode('NOT_FOUND');
        }
        else {
          rsp.rsc = m2m.code.getResponseStatusCode('BAD_REQUEST');
        }
      }

      // Unknown database
      else {
        rsp.rsc = m2m.code.getResponseStatusCode('INTERNAL_SERVER_ERROR');
      }

      if (callback) callback(rsp);
    });
  }

  db.retrieve(req.to, undefined, function (err, rows) {
    var parentPath = req.to.slice(0, req.to.lastIndexOf('/'));
    if (parentPath.lastIndexOf('/') > 0) {
      var delta;
      if (!err && rows && rows.length > 0 && rows[0].content) {
        delta = -rows[0].contentsize;
      }
      db.retrieve(parentPath, undefined, function (err, rows) {
        if (!err) deleteResource(rows[0], delta);
      });
    }
    else {
      deleteResource();
    }
  });
}
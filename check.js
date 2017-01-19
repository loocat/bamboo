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

var m2m = require('onem2m');
var log = require('./logger').logger();

/**
 * 
 */  
var checkReturnContent = function (req) {

  if (('rcn' in req) === false) {
    // set default Result Content type: 'Attributes'
    req.rcn = m2m.code.getResultContent('Attributes');
  }
  
  if (req.op === m2m.code.getOperation('Retrieve')) {
    if (req.rcn === m2m.code.getResultContent('Nothing')) {
      // Retrieve request shall have response content except 'Nothing'
      return false;
    }
  }
  else if (req.rcn === m2m.code.getResultContent('Attributes and child resources')) {
    // This setting is only valid for a Retrieve operation.
    return false;
  }
  
  if (req.op !== m2m.code.getOperation('Create')) {
    if (req.rcn === m2m.code.getResultContent('Hierarchical address') ||
        req.rcn === m2m.code.getResultContent('Hierarchical address and attributes')) {
      // These settings shall be only valid for a Create operation
      return false;
    }
  }
  return true;
}

var resourceAttributeCheckList = {
  'accessControlPolicy': [
    // see table 7.4.2.1-2 Universal/Common Attributes of <group> resource (TS-0004 v4.2.7)
    {attribute: 'resourceName', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'resourceType', expected: false}, 
    {attribute: 'resourceID', expected: false},
    {attribute: 'parentID', expected: false},
    {attribute: 'expirationTime', expected: undefined},
    {attribute: 'labels', expected: undefined},
    {attribute: 'creationTime', expected: false},
    {attribute: 'lastModifiedTime', expected: false},
    {attribute: 'announceTo', expected: undefined},
    {attribute: 'announcedAttribute', expected: undefined},
    // sett table 7.4.2.1-3 Resource Specific Attributes of <group> resource (TS_0005 v4.2.7)
    {attribute: 'privileges', expected: (op) => { return (op === m2m.code.getOperation('Update') ? undefined : true); }},
    {attribute: 'selfPrivileges', expected: (op) => { return (op === m2m.code.getOperation('Update') ? undefined : true); }}
  ],
  'AE': [
    // see table 7.3.5.1-2 Universal/Common Attributes of <AE> resource (TS_118.104)
    {attribute: 'resourceName', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'resourceType', expected: false},
    {attribute: 'resourceID', expected: false},
    {attribute: 'parentID', expected: false},
    {attribute: 'accessControlPolicyIDs', expected: undefined},
    {attribute: 'creationTime', expected: false},
    {attribute: 'expirationTime', expected: undefined},
    {attribute: 'lastModifiedTime', expected: false},
    {attribute: 'labels', expected: undefined},
    {attribute: 'announceTo', expected: undefined},
    {attribute: 'announcedAttribute', expected: undefined},
    {attribute: 'dynamicAuthorizationConsultationIDs', expected: undefined},
    // see table 7.3.5.1-3 Resource Specific Attributes of <AE> resource (TS_118.104)
    {attribute: 'appName', expected: undefined},
    {attribute: 'App-ID', expected: function (op) { return (op === m2m.code.getOperation('Create') ? true : false); }},
    {attribute: 'AE-ID', expected: false},
    {attribute: 'pointOfAccess', expected: undefined},
    {attribute: 'ontologyRef', expected: undefined},
    {attribute: 'nodeLink', expected: false},
    {attribute: 'requestReachability', expected: (op) => { return (op === m2m.code.getOperation('Create') ? true : undefined); }},
    {attribute: 'contentSerialization', expected: undefined},
    {attribute: 'e2eSecInfo', expected: undefined}
  ],
  'container': [
    // see table 7.3.6.1-2 Universal/Common Attributes of <contaienr> resource (TS_118.104)
    {attribute: 'resourceName', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'resourceType', expected: false},
    {attribute: 'resourceID', expected: false},
    {attribute: 'parentID', expected: false},
    {attribute: 'accessControlPolicyIDs', expected: undefined},
    {attribute: 'creationTime', expected: false},
    {attribute: 'expirationTime', expected: undefined},
    {attribute: 'lastModifiedTime', expected: false},
    {attribute: 'stateTag', expected: false},
    {attribute: 'labels', expected: undefined},
    {attribute: 'announceTo', expected: undefined},
    {attribute: 'announcedAttribute', expected: undefined},
    {attribute: 'dynamicAuthorizationConsultationIDs', expected: undefined},
    {attribute: 'creator', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    // see table 7.3.6.1-3 Resource Specific Attributes of <container> resource (TS_118.104)
    {attribute: 'maxNrOfInstances', expected: undefined},
    {attribute: 'maxByteSize', expected: undefined},
    {attribute: 'maxInstanceAge', expected: undefined},
    {attribute: 'currentNrOfInstances', expected: false},
    {attribute: 'currentByteSize', expected: false},
    {attribute: 'locationID', expected: undefined},
    {attribute: 'ontologyRef', expected: undefined},
    {attribute: 'disableRetrieval', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }}
  ],
  'contentInstance': [
    // see table 7.3.7.1-2 Universal/Common Attributes of <contentInstance> resource (TS_118.104)
    {attribute: 'resourceName', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'resourceType', expected: false},
    {attribute: 'resourceID', expected: false},
    {attribute: 'parentID', expected: false},
    {attribute: 'expirationTime', expected: undefined},
    {attribute: 'creationTime', expected: false},
    {attribute: 'lastModifiedTime', expected: false},
    {attribute: 'stateTag', expected: false},
    {attribute: 'labels', expected: undefined},
    {attribute: 'announceTo', expected: undefined},
    {attribute: 'announcedAttribute', expected: undefined},
    {attribute: 'dynamicAuthorizationConsultationIDs', expected: undefined},
    {attribute: 'creator', expected: undefined},
    // see table 7.3.7.1-3 Resource Specific Attributes of <contentInstance> resource (TS_118.104)
    {attribute: 'contentInfo', expected: undefined},
    {attribute: 'contentSize', expected: function (op) { return (op === m2m.code.getOperation('Create') ? false : undefined); }},
    {attribute: 'contentRef', expected: undefined},
    {attribute: 'ontologyRef', expected: undefined},
    {attribute: 'content', expected: true},
  ],
  'CSEBase': [
    // // see table 7.3.3.1-2 Universal/Common Attributes of <CSEBase> resource (TS_118.104)
    // {attribute: 'resourceName', expected: false},
    // {attribute: 'resourceType', expected: false},
    // {attribute: 'resourceID', expected: false},
    // {attribute: 'parentID', expected: false},
    // {attribute: 'accessControlPolicyIDs', expected: false},
    // {attribute: 'creationTime', expected: false},
    // {attribute: 'lastModifiedTime', expected: false},
    // {attribute: 'labels', expected: false},
    // check private attributes
    // see table 7.3.5.1-3 Resource Specific Attributes of <CSEBase> resource (TS_118.104)
    {attribute: 'cseType', expected: false},
    {attribute: 'CSE-ID', expected: false},
    {attribute: 'supportedResourceType', expected: false},
    {attribute: 'pointOfAccess', expected: undefined},
    {attribute: 'nodeLink', expected: undefined},
    {attribute: 'dynamicAuthorizationConsultationIDs', expected: undefined},
    {attribute: 'e2eSecInfo', expected: undefined}
  ],
  'group': [
    // see table 7.4.13.1-2 Universal/Common Attributes of <group> resource (TS-0004 v4.2.7)
    {attribute: 'resourceName', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'resourceType', expected: false}, 
    {attribute: 'resourceID', expected: false},
    {attribute: 'parentID', expected: false},
    {attribute: 'accessControlPolicyIDs', expected: undefined},
    {attribute: 'creationTime', expected: false},
    {attribute: 'expirationTime', expected: undefined},
    {attribute: 'lastModifiedTime', expected: false},
    {attribute: 'labels', expected: undefined},
    {attribute: 'announceTo', expected: undefined},
    {attribute: 'announcedAttribute', expected: undefined},
    {attribute: 'creator', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'dynamicAuthorizationConsultationIDs', expected: undefined},
    // see table 7.4.13.1-3 Resource Specific Attributes of <group> resource (TS_0005 v4.2.7)
    {attribute: 'memberType', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'currentNrOfMembers', expected: false},
    {attribute: 'maxNrOfMembers', expected: (op) => { return (op === m2m.code.getOperation('Update') ? undefined : true); }},
    {attribute: 'memberIDs', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : true); }},
    {attribute: 'memberAccessControlPolicyIDs', expected: undefined},
    {attribute: 'memberTypeValidated', expected: false},
    {attribute: 'consistencyStrategy', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'groupName', expected: undefined},
  ],
  'remoteCSE': [
    // see table 7.3.4.1-2 Universal/Common Attributes of <remoteCSE> resource (TS_118.104)
    {attribute: 'resourceName', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'resourceType', expected: false},
    {attribute: 'resourceID', expected: false},
    {attribute: 'parentID', expected: false},
    {attribute: 'accessControlPolicyIDs', expected: undefined},
    {attribute: 'creationTime', expected: false},
    {attribute: 'expirationTime', expected: undefined},
    {attribute: 'lastModifiedTime', expected: false},
    {attribute: 'labels', expected: undefined},
    {attribute: 'announceTo', expected: undefined},
    {attribute: 'announcedAttribute', expected: undefined},
    {attribute: 'dynamicAuthorizationConsultationIDs', expected: undefined},
    // see table 7.3.4.1-3 Resource Specific Attributes of <remoteCSE> resource (TS_118.104)
    {attribute: 'cseType', expected: function (op) { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'pointOfAccess', expected: undefined},
    {attribute: 'CSEBase', expected: function (op) { return (op === m2m.code.getOperation('Create') ? true : false); }},
    {attribute: 'CSE-ID', expected: function (op) { return (op === m2m.code.getOperation('Create') ? true : false); }},
    {attribute: 'M2M-Ext-ID', expected: undefined},
    {attribute: 'Trigger-Recipient-ID', expected: undefined},
    {attribute: 'requestReachability', function (op) { return (op === m2m.code.getOperation('Create') ? true : undefined); }},
    {attribute: 'nodeLink', expected: undefined},
    {attribute: 'triggerReferenceNumber', expected: undefined},
    {attribute: 'e2eSecInfo', expected: undefined}
  ],
  'subscription': [
    // see table 7.3.8.1-2 Universal/Common Attributes of <subscription> resource (TS_118.104)
    {attribute: 'resourceName', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'resourceType', expected: false},
    {attribute: 'resourceID', expected: false},
    {attribute: 'parentID', expected: false},
    {attribute: 'accessControlPolicyIDs', expected: undefined},
    {attribute: 'creationTime', expected: false},
    {attribute: 'expirationTime', expected: undefined},
    {attribute: 'lastModifiedTime', expected: false},
    {attribute: 'labels', expected: undefined},
    {attribute: 'creator', expected: (op) => { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'dynamicAuthorizationConsultationIDs', expected: undefined},
    // see table 7.3.8.1-3 Resource Specific Attributes of <subscription> resource (TS_118.104)
    {attribute: 'eventNotificationCriteria', expected: undefined},
    {attribute: 'expirationCounter', expected: undefined},
    {attribute: 'notificationURI', expected: function (op) { return (op === m2m.code.getOperation('Create') ? true : undefined); }},
    {attribute: 'groupID', expected: undefined},
    {attribute: 'notificationForwardingURI', expected: undefined},
    {attribute: 'batchNotify', expected: undefined},
    {attribute: 'rateLimit', expected: undefined},
    {attribute: 'preSubscriptionNotify', expected: function (op) { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
    {attribute: 'pendingNotification', expected: undefined},
    {attribute: 'notificationStoragePriority', expected: undefined},
    {attribute: 'latestNotify', expected: undefined},
    {attribute: 'notificationContentType', expected: undefined},
    {attribute: 'notificationEventCat', expected: undefined},
    {attribute: 'subscriberURI', expected: function (op) { return (op === m2m.code.getOperation('Update') ? false : undefined); }},
  ]
};

//
// check resource attributes given
//
var checkResourceAttributes = function (op, ty, res) {
  
  var exist = function (attr) {
    return (typeof res[m2m.name.getShort(attr, ['ResourceAttributes', 'ComplexDataTypesMembers'])] !== 'undefined');
  }
  
  var checkViolations = function (list) {
    var res;
    if (list) {
      for (var ii in list) {
        var expected = list[ii].expected;
        if (typeof expected === 'function') { expected = expected(op); }
        if (typeof expected !== 'undefined' && exist(list[ii].attribute) !== expected) {
          if (!res) { res = []; }
          res.push(list[ii].attribute + (expected ? ' is not present' : ' is present'));
        }
      }
    }
    return (res ? 'resource attribute violation(s): ' + res : undefined);
  }

  return checkViolations(resourceAttributeCheckList[m2m.code.getResourceType(ty)]);
}

/**
 * 
 */
exports.checkRequest = function (req) {

  if (('op' in req) === false || !req.op) {
    return 'missing operation type';
  }
  else if (req.op < m2m.code.getOperation('Create') || req.op > m2m.code.getOperation('Notify')) {
    return 'invalid operation type: ' + req.op;
  }
  
  if (!req.rqi || req.rqi.length < 1) {
    return 'empty request identifier';
  }
  
  if (!req.fr || req.fr.length < 1) {
    return 'empty request originator';
  }
  
  if (!req.to || req.to.length < 1) {
    return 'empty target (resource path)';
  }
  
  if (!checkReturnContent(req)) {
    return 'invalid ResultContent type: ' + m2m.code.getResultContent(req.rcn);
  }

  if (req.to.match(/\/la$/)) {
    if (req.op === m2m.code.getOperation('Create') || req.op === m2m.code.getOperation('Update') || req.fc) {
      // possible operations on 'latest' virtual resource: Retrieve, Delete
      return 'invalid operation on \'latest\' virtual resource: ' + m2m.code.getOperation(req.op);
    }
    if (req.ty && !isPossibleChild(req.ty, m2m.code.getResourceType('contentInstance'))) {
      // 'latest' virtual resource is not available
      return 'invalid parent resource type for \'latest\': ' + m2m.code.getResourceType(req.ty);
    }
  }
  
  if (req.to.match(/\/fopt$/)) {
    if (req.ty && !isPossibleChild(req.ty, m2m.code.getResourceType('fanOutPoint'))) {
      // 'fanOutPoint' virtual resource is not available
      return 'invalid parent resource type for \'fanOutPoint\': ' + m2m.code.getResourceType(req.ty);
    }
  }

  if (!req.ty || req.ty < 1) {
    if (req.op === m2m.code.getOperation('Create')) {
      // resource type is required for the Create operation
      return 'missing resource type attribute for the operation \'' + m2m.code.getOperation(req.op) + '\'';
    }
    else if (req.op === m2m.code.getOperation('Update')) {
      // resource type is inconsistent for the Update operation
      return 'inconsistent resource structure for the operation \'' + m2m.code.getOperation(req.op) + '\'';
    }
  }
  else {
    // check if the operation is allowed for the resource type
    if (req.ty === m2m.code.getResourceType('CSEBase')) {
      if (req.op !== m2m.code.getOperation('Retrieve')) {
        return '\'CSEBase\' resource is not allowed for \'' + m2m.code.getOperation(req.op) + '\' operation';  
      }
    }
    else if (req.ty === m2m.code.getResourceType('contentInstance')) {
      if (req.op === m2m.code.getOperation('Update')) {
        return '\'contentInstance\' resource is not allowed for \'' + m2m.code.getOperation(req.op) + '\' operation';
      }
    }
  }
  
  if (!req.pc) {
    if (req.op === m2m.code.getOperation('Create') || req.op === m2m.code.getOperation('Update')) {
      // req.pc shall be present
      return 'missing resource structure for \'' + m2m.code.getOperation(req.op) + '\' operation';
    }
  }
  
  if (req.ty && req.pc && (req.op === m2m.code.getOperation('Create') || req.op === m2m.code.getOperation('Update'))) {
    // try to get the resource structure from the Create/Update operation
    var resource = req.pc[m2m.name.getShort(m2m.code.getResourceType(req.ty), 'ResourceTypes')]; 
    if (!resource) {
      // req.pc does not include corresponding resource structure for the Create/Update operation
      return 'missing resource structure for \'' + m2m.code.getResourceType(req.ty) + '\'';
    }
  
    // check resource attributes given
    var err = checkResourceAttributes(req.op, req.ty, resource);
    if (err) { return err; }
  }

  //
  // TODO check additional constraints...
  //

  return undefined;
}


var childResourceList = {
  'accessControlPolicy': [
    // see table 7.4.2.1-4 Child resources of <accessControlPolicy> resource (TS-0004 v2.7.1)
    m2m.code.getResourceType('subscription')
  ],
  'AE': [
    // see table 7.4.5.1-4 Child resources of <AE> resource (TS-0004 v2.7.1)
    m2m.code.getResourceType('subscription'),
    m2m.code.getResourceType('container'),
    m2m.code.getResourceType('group'),
    m2m.code.getResourceType('accessControlPolicy'),
    m2m.code.getResourceType('pollingChannel'),
    m2m.code.getResourceType('schedule'),
    m2m.code.getResourceType('semanticDescriptor'),
    m2m.code.getResourceType('dynamicAuthorizationConsultation'),
    m2m.code.getResourceType('flexContainer'),
    m2m.code.getResourceType('timeSeries'),
    m2m.code.getResourceType('trafficPattern')
  ],
  'container': [
    // see table 7.4.6.1-4 Child resources of <container> resource (TS-0004 v2.7.1)
    m2m.code.getResourceType('contentInstance'),
    m2m.code.getResourceType('subscription'),
    m2m.code.getResourceType('container'),
    m2m.code.getResourceType('latest'),
    m2m.code.getResourceType('oldest'),
    m2m.code.getResourceType('semanticDescriptor'),
    m2m.code.getResourceType('flexContainer')
  ],
  'contentInstance': [
    // see table 7.4.7.1-4 Child resources of <contentInstance> resource (TS-0004 v2.7.1)
    m2m.code.getResourceType('semanticDescriptor')
  ],
  'CSEBase': [
    // see table 7.4.3.1-3 Child resources of <CSEBase> resource (TS-0004 v2.7.1)
    m2m.code.getResourceType('remoteCSE'),
    m2m.code.getResourceType('remoteCSEAnnc'),
    m2m.code.getResourceType('node'),
    m2m.code.getResourceType('AE'),
    m2m.code.getResourceType('container'),
    m2m.code.getResourceType('group'),
    m2m.code.getResourceType('accessControlPolicy'),
    m2m.code.getResourceType('subscription'),
    m2m.code.getResourceType('mgmtCmd'),
    m2m.code.getResourceType('locationPolicy'),
    m2m.code.getResourceType('statsConfig'),
    m2m.code.getResourceType('statsCollect'),
    m2m.code.getResourceType('request'),
    m2m.code.getResourceType('delivery'),
    m2m.code.getResourceType('schedule'),
    m2m.code.getResourceType('m2mServiceSubscriptionPolicy'),
    m2m.code.getResourceType('serviceSubscribedAppRule'),
    m2m.code.getResourceType('notificationTargetPolicy'),
    m2m.code.getResourceType('dynamicAuthorizationConsultation'),
    m2m.code.getResourceType('flexContainer'),
    m2m.code.getResourceType('timeSeries'),
    m2m.code.getResourceType('role'),
    m2m.code.getResourceType('token')
  ],
  'group': [
    // see table 7.4.13.1-4 Child resources of <group> resource (TS-0004 v2.7.1)
    m2m.code.getResourceType('subscription'),
    m2m.code.getResourceType('semanticDescriptor'),
    m2m.code.getResourceType('fanOutPoint'),
    // m2m.code.getResourceType('semanticFanOutPoint')
  ],
  'remoteCSE': [
    // see table 7.4.4.1-4 Child resources of <CSEBase> resource (TS-0004 v2.7.1)
    m2m.code.getResourceType('container'),
    m2m.code.getResourceType('containerAnnc'),
    m2m.code.getResourceType('flexContainer'),
    m2m.code.getResourceType('flexContainerAnnc'),
    m2m.code.getResourceType('group'),
    m2m.code.getResourceType('groupAnnc'),
    m2m.code.getResourceType('accessControlPolicy'),
    m2m.code.getResourceType('accessControlPolicyAnnc'),
    m2m.code.getResourceType('subscription'),
    m2m.code.getResourceType('pollingChannel'),
    m2m.code.getResourceType('schedule'),
    m2m.code.getResourceType('nodeAnnc'),
    m2m.code.getResourceType('timeSeries'),
    m2m.code.getResourceType('timeSeriesAnnc'),
    m2m.code.getResourceType('remoteCSEAnnc'),
    m2m.code.getResourceType('AEAnnc'),
    m2m.code.getResourceType('locationPolicyAnnc')
  ],
  'subscription': [
    // see table 7.4.8.1-4 Child resources of <subscription> resource (TS-0004 v2.7.1)
    m2m.code.getResourceType('schedule'),
    m2m.code.getResourceType('notificationTargetMgmtPolicyRef'),
    // m2m.code.getResourceType('notificationTargetSelfReference')
  ]
};

/**
 * 
 */
var isPossibleChild = function (pty, cty) {
  var list = childResourceList[m2m.code.getResourceType(pty)];
  if (Array.isArray(list)) {
    for (var ii = 0; ii < list.length; ++ii) {
      if (list[ii] === cty) return true;
    }    
  }
  return false;
}

exports.isPossibleChild = isPossibleChild;
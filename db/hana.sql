DROP TABLE "LV" CASCADE;

CREATE ROW TABLE "LV" (
	 "parentpath" NVARCHAR(255) CS_STRING,
	 "path" NVARCHAR(255) CS_STRING NOT NULL,
	 "parentid" NVARCHAR(45) CS_STRING,
	 "resourcetype" INT CS_INT NOT NULL,
	 "resourceid" NVARCHAR(45) CS_STRING NOT NULL,
	 "resourcename" NVARCHAR(45) CS_STRING NOT NULL,
	 "creationtime" NVARCHAR(45) CS_STRING NOT NULL,
	 "lastmodifiedtime" NVARCHAR(45) CS_STRING NOT NULL,
	 "expirationtime" NVARCHAR(45) CS_STRING,
	 "accesscontrolpolicyids" NVARCHAR(500) CS_STRING,
	 "labels" NVARCHAR(45) CS_STRING,
	 "csetype" INT CS_INT,
	 "cseid" NVARCHAR(45) CS_STRING,
	 "supportedresourcetype" NVARCHAR(45) CS_STRING,
	 "pointofaccess" NVARCHAR(255) CS_STRING,
	 "nodelink" NVARCHAR(45) CS_STRING,
	 "announceto" NVARCHAR(45) CS_STRING,
	 "announcedattribute" NVARCHAR(45) CS_STRING,
	 "csebase" NVARCHAR(45) CS_STRING,
	 "m2mextid" NVARCHAR(45) CS_STRING,
	 "triggerrecipientid" INT CS_INT,
	 "requestreachability" NVARCHAR(45) CS_STRING,
	 "appname" NVARCHAR(45) CS_STRING,
	 "appid" NVARCHAR(45) CS_STRING,
	 "aeid" NVARCHAR(45) CS_STRING,
	 "ontologyref" NVARCHAR(45) CS_STRING,
	 "statetag" INT CS_INT,
	 "creator" NVARCHAR(45) CS_STRING,
	 "maxnrofinstances" INT CS_INT,
	 "maxbytesize" INT CS_INT,
	 "maxinstanceage" INT CS_INT,
	 "currentnrofinstances" INT CS_INT,
	 "currentbytesize" INT CS_INT,
	 "locationid" NVARCHAR(45) CS_STRING,
	 "contentinfo" NVARCHAR(45) CS_STRING,
	 "contentsize" INT CS_INT,
	 "content" CLOB CS_STRING,
	 "eventnotificationcriteria" NVARCHAR(500) CS_STRING,
	 "notificationuri" NVARCHAR(255) CS_STRING,
	 "groupid" NVARCHAR(45) CS_STRING,
	 "notificationforwardinguri" NVARCHAR(45) CS_STRING,
	 "batchnotify" NVARCHAR(45) CS_STRING,
	 "ratelimit" NVARCHAR(45) CS_STRING,
	 "pendingnotification" INT CS_INT,
	 "notificationstoragepriority" NVARCHAR(45) CS_STRING,
	 "latestnotify" NVARCHAR(45) CS_STRING,
	 "notificationcontenttype" INT CS_INT,
	 "notificationeventcat" INT CS_INT,
	 "expirationcounter" INT CS_INT,
	 "presubscriptionnotify" INT CS_INT,
	 "subscriberuri" NVARCHAR(45) CS_STRING,
	 PRIMARY KEY ("resourceid")
) ;
 
CREATE UNIQUE INDEX UK_LV_PATH ON "LV" ("path");
 
CREATE INDEX IX_LV_COMP ON "LV" ("parentpath", "resourcetype", "creationtime" DESC);

CREATE INDEX IX_LV_CREATIONTIME ON "LV" ("creationtime" DESC);

CREATE INDEX IX_LV_LASTMODIFIEDTIME ON "LV" ("lastmodifiedtime" DESC);

CREATE INDEX IX_LV_EXPIRATIONTIME ON "LV" ("expirationtime" DESC);
USE crossflow;

DROP TABLE IF EXISTS lv;

CREATE TABLE lv (
  parentpath varchar(255) DEFAULT NULL,
  path varchar(255) NOT NULL,
  parentid varchar(45) DEFAULT NULL,
  resourcetype int NOT NULL,
  resourceid varchar(45) NOT NULL,
  resourcename varchar(45) NOT NULL,
  creationtime varchar(45) NOT NULL,
  lastmodifiedtime varchar(45) NOT NULL,
  expirationtime varchar(45) DEFAULT NULL,
  accesscontrolpolicyids varchar(500) DEFAULT NULL,
  labels varchar(500) DEFAULT NULL,
  csetype int DEFAULT NULL,
  cseid varchar(45) DEFAULT NULL,
  supportedresourcetype varchar(45) DEFAULT NULL,
  pointofaccess varchar(255) DEFAULT NULL,
  nodelink varchar(45) DEFAULT NULL,
  announceto varchar(45) DEFAULT NULL,
  announcedattribute varchar(45) DEFAULT NULL,
  csebase varchar(45) DEFAULT NULL,
  m2mextid varchar(45) DEFAULT NULL,
  triggerrecipientid int DEFAULT NULL,
  requestreachability varchar(45) DEFAULT NULL,
  appname varchar(45) DEFAULT NULL,
  appid varchar(45) DEFAULT NULL,
  aeid varchar(45) DEFAULT NULL,
  ontologyref varchar(45) DEFAULT NULL,
  statetag int DEFAULT NULL,
  creator varchar(255) DEFAULT NULL,
  maxnrofinstances int DEFAULT NULL,
  maxbytesize int DEFAULT NULL,
  maxinstanceage int DEFAULT NULL,
  currentnrofinstances int DEFAULT NULL,
  currentbytesize int DEFAULT NULL,
  locationid varchar(45) DEFAULT NULL,
  contentinfo varchar(45) DEFAULT NULL,
  contentsize int DEFAULT NULL,
  content mediumtext DEFAULT NULL,
  eventnotificationcriteria varchar(500) DEFAULT NULL,
  notificationuri varchar(255) DEFAULT NULL,
  groupid varchar(45) DEFAULT NULL,
  notificationforwardinguri varchar(45) DEFAULT NULL,
  batchnotify varchar(45) DEFAULT NULL,
  ratelimit varchar(45) DEFAULT NULL,
  pendingnotification int DEFAULT NULL,
  notificationstoragepriority varchar(45) DEFAULT NULL,
  latestnotify varchar(45) DEFAULT NULL,
  notificationcontenttype int DEFAULT NULL,
  notificationeventcat int DEFAULT NULL,
  expirationcounter int DEFAULT NULL,
  presubscriptionnotify int DEFAULT NULL,
  subscriberuri varchar(45) DEFAULT NULL,
  membertype int DEFAULT NULL,
  memberids varchar(500) DEFAULT NULL,
  maxnrofmembers int DEFAULT NULL,
  privileges varchar(500) DEFAULT NULL,
  selfprivileges varchar(500) DEFAULT NULL,
  PRIMARY KEY (resourceid),
  UNIQUE KEY uk_lv_path (path),
  KEY ix_lv_comp (parentpath, resourcetype, creationtime DESC),
  KEY ix_lv_creationtime (creationtime),
  KEY ix_lv_lastmodifiedtime (lastmodifiedtime),
  KEY ix_lv_expirationtime (expirationtime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP FUNCTION IF EXISTS get_lvl_lv;

DELIMITER $$

CREATE FUNCTION  get_lvl_lv(value VARCHAR(100)) RETURNS varchar(100)
NOT DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_resourceid varchar(100);
    DECLARE v_parent varchar(100);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET @resourceid = NULL;
 
    SET v_parent = @resourceid;
    SET v_resourceid = '';
 
    IF @resourceid IS NULL THEN
        RETURN NULL;
    END IF;
 
    LOOP
        SELECT MIN(resourceid) INTO @resourceid FROM lv
        WHERE parentid = v_parent AND resourceid > v_resourceid;
                
        IF @resourceid IS NOT NULL OR v_parent = @start_with THEN
            SET @level = @level + 1;
            RETURN @resourceid;
        END IF;
            
        SET @level := @level - 1;
            
        SELECT resourceid, parentid INTO v_resourceid , v_parent FROM lv
        WHERE resourceid = v_parent;
    END LOOP;
END
$$
DELIMITER ;
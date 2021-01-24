# ************************************************************
# Sequel Pro SQL dump
# Version 4541
#
# http://www.sequelpro.com/
# https://github.com/sequelpro/sequelpro
#
# Host: 0.0.0.0 (MySQL 5.5.40-36.1-log)
# Database: ipp_azkaban
# Generation Time: 2019-12-06 09:12:08 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table active_executing_flows
# ------------------------------------------------------------

DROP TABLE IF EXISTS `active_executing_flows`;

CREATE TABLE `active_executing_flows` (
  `exec_id` int(11) NOT NULL DEFAULT '0',
  `host` varchar(255) DEFAULT NULL,
  `port` int(11) DEFAULT NULL,
  `update_time` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`exec_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table active_sla
# ------------------------------------------------------------

DROP TABLE IF EXISTS `active_sla`;

CREATE TABLE `active_sla` (
  `exec_id` int(11) NOT NULL,
  `job_name` varchar(128) NOT NULL,
  `check_time` bigint(20) NOT NULL,
  `rule` tinyint(4) NOT NULL,
  `enc_type` tinyint(4) DEFAULT NULL,
  `options` longblob NOT NULL,
  PRIMARY KEY (`exec_id`,`job_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table execution_flows
# ------------------------------------------------------------

DROP TABLE IF EXISTS `execution_flows`;

CREATE TABLE `execution_flows` (
  `exec_id` int(11) NOT NULL AUTO_INCREMENT,
  `project_id` int(11) NOT NULL,
  `version` int(11) NOT NULL,
  `flow_id` varchar(128) NOT NULL,
  `status` tinyint(4) DEFAULT NULL,
  `submit_user` varchar(64) DEFAULT NULL,
  `submit_time` bigint(20) DEFAULT NULL,
  `update_time` bigint(20) DEFAULT NULL,
  `start_time` bigint(20) DEFAULT NULL,
  `end_time` bigint(20) DEFAULT NULL,
  `enc_type` tinyint(4) DEFAULT NULL,
  `flow_data` longblob,
  PRIMARY KEY (`exec_id`),
  KEY `ex_flows_start_time` (`start_time`),
  KEY `ex_flows_end_time` (`end_time`),
  KEY `ex_flows_time_range` (`start_time`,`end_time`),
  KEY `ex_flows_flows` (`project_id`,`flow_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table execution_jobs
# ------------------------------------------------------------

DROP TABLE IF EXISTS `execution_jobs`;

CREATE TABLE `execution_jobs` (
  `exec_id` int(11) NOT NULL,
  `project_id` int(11) NOT NULL,
  `version` int(11) NOT NULL,
  `flow_id` varchar(128) NOT NULL,
  `job_id` varchar(128) NOT NULL,
  `attempt` int(11) NOT NULL DEFAULT '0',
  `start_time` bigint(20) DEFAULT NULL,
  `end_time` bigint(20) DEFAULT NULL,
  `status` tinyint(4) DEFAULT NULL,
  `input_params` longblob,
  `output_params` longblob,
  `attachments` longblob,
  PRIMARY KEY (`exec_id`,`job_id`,`attempt`),
  KEY `exec_job` (`exec_id`,`job_id`),
  KEY `exec_id` (`exec_id`),
  KEY `ex_job_id` (`project_id`,`job_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table execution_logs
# ------------------------------------------------------------

DROP TABLE IF EXISTS `execution_logs`;

CREATE TABLE `execution_logs` (
  `exec_id` int(11) NOT NULL,
  `name` varchar(128) NOT NULL DEFAULT '',
  `attempt` int(11) NOT NULL DEFAULT '0',
  `enc_type` tinyint(4) DEFAULT NULL,
  `start_byte` int(11) NOT NULL DEFAULT '0',
  `end_byte` int(11) DEFAULT NULL,
  `log` longblob,
  `upload_time` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`exec_id`,`name`,`attempt`,`start_byte`),
  KEY `ex_log_attempt` (`exec_id`,`name`,`attempt`),
  KEY `ex_log_index` (`exec_id`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table project_events
# ------------------------------------------------------------

DROP TABLE IF EXISTS `project_events`;

CREATE TABLE `project_events` (
  `project_id` int(11) NOT NULL,
  `event_type` tinyint(4) NOT NULL,
  `event_time` bigint(20) NOT NULL,
  `username` varchar(64) DEFAULT NULL,
  `message` varchar(512) DEFAULT NULL,
  KEY `log` (`project_id`,`event_time`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table project_files
# ------------------------------------------------------------

DROP TABLE IF EXISTS `project_files`;

CREATE TABLE `project_files` (
  `project_id` int(11) NOT NULL,
  `version` int(11) NOT NULL,
  `chunk` int(11) NOT NULL DEFAULT '0',
  `size` int(11) DEFAULT NULL,
  `file` longblob,
  PRIMARY KEY (`project_id`,`version`,`chunk`),
  KEY `file_version` (`project_id`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table project_flows
# ------------------------------------------------------------

DROP TABLE IF EXISTS `project_flows`;

CREATE TABLE `project_flows` (
  `project_id` int(11) NOT NULL,
  `version` int(11) NOT NULL,
  `flow_id` varchar(128) NOT NULL DEFAULT '',
  `modified_time` bigint(20) NOT NULL,
  `encoding_type` tinyint(4) DEFAULT NULL,
  `json` blob,
  PRIMARY KEY (`project_id`,`version`,`flow_id`),
  KEY `flow_index` (`project_id`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table project_permissions
# ------------------------------------------------------------

DROP TABLE IF EXISTS `project_permissions`;

CREATE TABLE `project_permissions` (
  `project_id` varchar(64) NOT NULL,
  `modified_time` bigint(20) NOT NULL,
  `name` varchar(64) NOT NULL,
  `permissions` int(11) NOT NULL,
  `isGroup` tinyint(1) NOT NULL,
  PRIMARY KEY (`project_id`,`name`),
  KEY `permission_index` (`project_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table project_properties
# ------------------------------------------------------------

DROP TABLE IF EXISTS `project_properties`;

CREATE TABLE `project_properties` (
  `project_id` int(11) NOT NULL,
  `version` int(11) NOT NULL,
  `name` varchar(255) NOT NULL DEFAULT '',
  `modified_time` bigint(20) NOT NULL,
  `encoding_type` tinyint(4) DEFAULT NULL,
  `property` blob,
  PRIMARY KEY (`project_id`,`version`,`name`),
  KEY `properties_index` (`project_id`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table project_versions
# ------------------------------------------------------------

DROP TABLE IF EXISTS `project_versions`;

CREATE TABLE `project_versions` (
  `project_id` int(11) NOT NULL,
  `version` int(11) NOT NULL,
  `upload_time` bigint(20) NOT NULL,
  `uploader` varchar(64) NOT NULL,
  `file_type` varchar(16) DEFAULT NULL,
  `file_name` varchar(128) DEFAULT NULL,
  `md5` binary(16) DEFAULT NULL,
  `num_chunks` int(11) DEFAULT NULL,
  PRIMARY KEY (`project_id`,`version`),
  KEY `version_index` (`project_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table projects
# ------------------------------------------------------------

DROP TABLE IF EXISTS `projects`;

CREATE TABLE `projects` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `active` tinyint(1) DEFAULT NULL,
  `modified_time` bigint(20) NOT NULL,
  `create_time` bigint(20) NOT NULL,
  `version` int(11) DEFAULT NULL,
  `last_modified_by` varchar(64) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `enc_type` tinyint(4) DEFAULT NULL,
  `settings_blob` longblob,
  PRIMARY KEY (`id`),
  UNIQUE KEY `project_id` (`id`),
  KEY `project_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table properties
# ------------------------------------------------------------

DROP TABLE IF EXISTS `properties`;

CREATE TABLE `properties` (
  `name` varchar(64) NOT NULL,
  `type` int(11) NOT NULL,
  `modified_time` bigint(20) NOT NULL,
  `value` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`name`,`type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table schedules
# ------------------------------------------------------------

DROP TABLE IF EXISTS `schedules`;

CREATE TABLE `schedules` (
  `schedule_id` int(11) NOT NULL AUTO_INCREMENT,
  `project_id` int(11) NOT NULL,
  `project_name` varchar(128) NOT NULL,
  `flow_name` varchar(128) NOT NULL,
  `status` varchar(16) DEFAULT NULL,
  `first_sched_time` bigint(20) DEFAULT NULL,
  `timezone` varchar(64) DEFAULT NULL,
  `period` varchar(16) DEFAULT NULL,
  `last_modify_time` bigint(20) DEFAULT NULL,
  `next_exec_time` bigint(20) DEFAULT NULL,
  `submit_time` bigint(20) DEFAULT NULL,
  `submit_user` varchar(128) DEFAULT NULL,
  `enc_type` tinyint(4) DEFAULT NULL,
  `schedule_options` longblob,
  PRIMARY KEY (`schedule_id`),
  KEY `sched_project_id` (`project_id`,`flow_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table triggers
# ------------------------------------------------------------

DROP TABLE IF EXISTS `triggers`;

CREATE TABLE `triggers` (
  `trigger_id` int(11) NOT NULL AUTO_INCREMENT,
  `trigger_source` varchar(128) DEFAULT NULL,
  `modify_time` bigint(20) NOT NULL,
  `enc_type` tinyint(4) DEFAULT NULL,
  `data` longblob,
  PRIMARY KEY (`trigger_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

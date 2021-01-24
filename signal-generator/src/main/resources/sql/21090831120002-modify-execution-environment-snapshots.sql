--liquibase formatted sql

--changeset rashmi.gulhane:1
CREATE TABLE `execution_environment_snapshots` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `execution_environment_id` bigint(20) NOT NULL,
  `library_set` text,
  `version` bigint(20) NOT NULL,
  `image_latest_digest` text,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_key_execution_enviornment_id_version` (`execution_environment_id`,`version`),
  UNIQUE KEY `UK5c8xau2avdfbk8jgrq0akk27q` (`execution_environment_id`,`version`),
  UNIQUE KEY `UK_5c8xau2avdfbk8jgrq0akk27q` (`execution_environment_id`,`version`),
  KEY `key_execution_enviornment_id_version` (`execution_environment_id`,`version`),
  CONSTRAINT `constraint_foreign_key_execution_enviornment_id_ibfk_1` FOREIGN KEY (`execution_environment_id`) REFERENCES `execution_environment` (`id`),
  CONSTRAINT `FKgmc48g0g3x7jjel4ggn37bhai` FOREIGN KEY (`execution_environment_id`) REFERENCES `execution_environment` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=latin1;
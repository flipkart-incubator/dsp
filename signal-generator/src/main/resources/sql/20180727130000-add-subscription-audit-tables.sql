--liquibase formatted sql

--changeset srikanth.vuppuluri:1
CREATE TABLE subscription_audits (
	id BIGINT(20) NOT NULL AUTO_INCREMENT
	,subscription_id varchar(255) NOT NULL
	,run_id BIGINT(20) NOT NULL
	,status VARCHAR(255) NOT NULL
	,created_at timestamp NULL DEFAULT NULL
	,updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	,PRIMARY KEY (`id`)
	,UNIQUE KEY `sa_unique`(`subscription_id`,`run_id`)
	) ENGINE = InnoDB DEFAULT CHARSET = utf8;
--rollback DROP TABLE subscription_audit;

--changeset srikanth.vuppuluri:2
CREATE TABLE subscription_audit_to_requests (
  id BIGINT(20) NOT NULL AUTO_INCREMENT
	,subscription_audit_id BIGINT(20) NOT NULL
	,request_id BIGINT(20) NOT NULL
	,created_at timestamp NULL DEFAULT NULL
	,updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	,PRIMARY KEY (`id`)
	,KEY `sa_request_id_key`(`request_id`)
	,KEY `sa_id_key`(`subscription_audit_id`)
	,CONSTRAINT `sa_request_id_key` FOREIGN KEY (`request_id`) REFERENCES `request`(`id`)
	,CONSTRAINT `sa_id_key` FOREIGN KEY (`subscription_audit_id`) REFERENCES `subscription_audits`(`id`)
	) ENGINE = InnoDB DEFAULT CHARSET = utf8;
--rollback DROP TABLE subscription_audit_to_requests;

--liquibase formatted sql

--changeset srikanth.vuppuluri:1
CREATE TABLE request_to_dataframe_audits (
	request_id BIGINT(20) NOT NULL
	,dataframe_audit_id INT(11) unsigned NOT NULL
	,workflow_id BIGINT(20) NOT NULL
	,created_at timestamp NULL DEFAULT NULL
	,updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	,KEY `request_id_key`(`request_id`)
	,KEY `dataframe_audit_key`(`dataframe_audit_id`)
	,KEY `workflow_id_key_2`(`workflow_id`)
	,CONSTRAINT `request_id_key` FOREIGN KEY (`request_id`) REFERENCES `request`(`id`)
	,CONSTRAINT `dataframe_audit_key` FOREIGN KEY (`dataframe_audit_id`) REFERENCES `dataframe_audit`(`run_id`)
	,CONSTRAINT `workflow_id_key_2` FOREIGN KEY (`workflow_id`) REFERENCES `workflow`(`id`)
	) ENGINE = InnoDB DEFAULT CHARSET = utf8;
--rollback DROP TABLE request_to_dataframe_audits;

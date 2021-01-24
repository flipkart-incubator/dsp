--liquibase formatted sql

--changeset srikanth.vuppuluri:1
CREATE TABLE workflow_to_dataframes (
	workflow_id BIGINT(20) NOT NULL COMMENT 'Unique identifier for workflow'
	,dataframe_id VARCHAR(50) NOT NULL COMMENT 'Unique identifier for dataframe'
	,created_at timestamp NULL DEFAULT NULL
	,updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	,KEY `workflow_id_key`(`workflow_id`)
	,KEY `dataframe_key`(`dataframe_id`)
	,CONSTRAINT `workflow_id_key` FOREIGN KEY (`workflow_id`) REFERENCES `workflow`(`id`)
	,CONSTRAINT `dataframe_key` FOREIGN KEY (`dataframe_id`) REFERENCES `dataframes`(`id`)
	) ENGINE = InnoDB DEFAULT CHARSET = utf8;
--rollback DROP TABLE workflow_to_dataframes;

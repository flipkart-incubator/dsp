--liquibase formatted sql

--changeset srikanth.vuppuluri:1
CREATE TABLE usecase_to_dataframes (
	sg_usecase VARCHAR(50) NOT NULL COMMENT 'Unique identifier for use case'
	,dataframe_id VARCHAR(50) NOT NULL COMMENT 'Unique identifier for dataframe'
	,created_at timestamp NULL DEFAULT NULL
	,updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	,KEY `sg_usecase_key`(`sg_usecase`)
	,KEY `dataframe_id_key`(`dataframe_id`)
	,CONSTRAINT `sg_usecase_key` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases`(`sg_usecase`)
	,CONSTRAINT `dataframe_id_key` FOREIGN KEY (`dataframe_id`) REFERENCES `dataframes`(`id`)
	) ENGINE = InnoDB DEFAULT CHARSET = utf8;
--rollback DROP TABLE usecase_to_dataframes;

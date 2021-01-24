--liquibase formatted sql

--changeset srikanth.vuppuluri:1
ALTER TABLE workflow_group
DROP COLUMN sg_usecases;
--rollback ALTER TABLE workflow_group add column sg_usecases varchar(255) DEFAULT NULL;

--changeset srikanth.vuppuluri:2
ALTER TABLE workflow_group ADD COLUMN is_draft TINYINT (1) DEFAULT 0
	,ADD COLUMN version DECIMAL(5, 2) DEFAULT 1
	,ADD COLUMN created_at TIMESTAMP NULL DEFAULT NULL
	,ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
--rollback ALTER TABLE workflow_group DROP COLUMN is_draft, DROP COLUMN version, DROP COLUMN created_at, DROP COLUMN updated_at;

--changeset srikanth.vuppuluri:3
ALTER TABLE workflow_group
DROP INDEX UKlrknunl8jdxo6daq3h2uluqg7
	,ADD UNIQUE KEY uniqueworkflowgroup(name, is_draft, version);
--rollback ALTER TABLE workflow_group ADD UNIQUE KEY UKlrknunl8jdxo6daq3h2uluqg7 (name), DROP INDEX uniqueworkflowgroup;
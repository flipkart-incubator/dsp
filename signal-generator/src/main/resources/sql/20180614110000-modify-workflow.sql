--liquibase formatted sql

--changeset srikanth.vuppuluri:1
ALTER TABLE workflow
DROP COLUMN execution_cluster;
--rollback alter table workflow add column execution_cluster varchar(255) NOT NULL;

--changeset srikanth.vuppuluri:2
ALTER TABLE workflow MODIFY ROLE enum('PRODUCTION','PVS_TRAIN','PVS_EXEC','EXPERIMENTATION') NOT NULL DEFAULT 'PRODUCTION';
--rollback alter table workflow MODIFY role enum('PRODUCTION','PVS_TRAIN','PVS_EXEC') NOT NULL DEFAULT 'PRODUCTION';

--changeset srikanth.vuppuluri:3
ALTER TABLE workflow ADD COLUMN is_draft TINYINT (1) DEFAULT 0
	,ADD COLUMN version DECIMAL(5, 2) DEFAULT 1
	,ADD COLUMN created_at TIMESTAMP NULL DEFAULT NULL
	,ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
--rollback ALTER TABLE workflow DROP COLUMN is_draft, DROP COLUMN version, DROP COLUMN created_at, DROP COLUMN updated_at;

--changeset srikanth.vuppuluri:4
ALTER TABLE workflow
DROP INDEX UK3je18ux0wru0pxv6un40yhbn4
	,ADD UNIQUE KEY uniqueworkflow(name, is_draft, version);
--rollback alter table workflow add UNIQUE KEY UK3je18ux0wru0pxv6un40yhbn4 (name), DROP INDEX uniqueworkflow;
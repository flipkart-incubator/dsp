--liquibase formatted sql

--changeset srikanth.vuppuluri:1
ALTER TABLE workflow_group ADD COLUMN is_active TINYINT (1) DEFAULT 1;
--rollback ALTER TABLE workflow_group DROP_COLUMN is_active;

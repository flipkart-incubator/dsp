--liquibase formatted sql

--changeset srikanth.vuppuluri:1
alter table workflow_audits
  MODIFY COLUMN created_at timestamp NULL DEFAULT NULL,
  MODIFY COLUMN updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

--liquibase formatted sql

--changeset srikanth.vuppuluri:1
alter table request_step
  MODIFY COLUMN created_at timestamp NULL DEFAULT NULL,
  MODIFY COLUMN updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

--changeset srikanth.vuppuluri:2
alter table request_step_audit
  MODIFY COLUMN created_at timestamp NULL DEFAULT NULL,
  MODIFY COLUMN updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
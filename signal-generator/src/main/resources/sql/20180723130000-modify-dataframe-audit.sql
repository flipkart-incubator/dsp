--liquibase formatted sql

--changeset srikanth.vuppuluri:1
alter table dataframe_audit
  DROP COLUMN request_id;
--rollback alter table dataframe_audit ADD COLUMN int(11) NOT NULL COMMENT 'Request id';
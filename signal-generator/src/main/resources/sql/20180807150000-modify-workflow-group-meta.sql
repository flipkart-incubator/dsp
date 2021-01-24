--liquibase formatted sql

--changeset srikanth.vuppuluri:1
ALTER TABLE workflow_group_meta ADD COLUMN azkaban_project varchar(255);
--rollback ALTER TABLE workflow_group_meta DROP_COLUMN azkaban_project;

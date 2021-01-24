--liquibase formatted sql

--changeset srikanth.vuppuluri:1
Alter table workflow_group_meta drop column mandatory_fields;

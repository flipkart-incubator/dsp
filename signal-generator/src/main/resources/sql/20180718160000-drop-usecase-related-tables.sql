--liquibase formatted sql

--changeset srikanth.vuppuluri:1
DROP TABLE usecase_to_dataframes;

--changeset srikanth.vuppuluri:2
DROP TABLE sg_usecases;

--changeset srikanth.vuppuluri:3
DROP TABLE workflow_meta;

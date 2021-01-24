--liquibase formatted sql

--changeset rashmi.gulhane:1
ALTER TABLE `dataframes` DROP COLUMN sg_type, DROP COLUMN dcp_table_name;
--liquibase formatted sql

--changeset ravikiran.kalal:1
ALTER TABLE `dataframe_audit` ADD `dataframe_size` bigint(20) NULL  DEFAULT 0 AFTER `dashboard_title`;

--changeset ravikiran.kalal:2
ALTER TABLE `dataframe_audit` ADD `input_partitions`  varchar(255) NULL  DEFAULT NULL AFTER `dataframe_size`;

--changeset ravikiran.kalal:3
ALTER TABLE `dataframe_audit` ADD `override_audit_id` int(11) NULL  DEFAULT NULL AFTER `input_partitions`;


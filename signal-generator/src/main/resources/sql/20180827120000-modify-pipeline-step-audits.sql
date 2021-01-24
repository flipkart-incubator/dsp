--liquibase formatted sql

--changeset ravikiran.kalal:1
ALTER TABLE `pipeline_step_audits` ADD `logs` VARCHAR(500)  NULL  DEFAULT NULL  AFTER `resources`;
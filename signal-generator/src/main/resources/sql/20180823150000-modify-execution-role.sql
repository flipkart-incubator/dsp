--liquibase formatted sql

--changeset dheeraj.khatri:1
ALTER TABLE workflow MODIFY ROLE enum('PRODUCTION','PVS_TRAIN','PVS_EXEC','EXPERIMENTATION','IWIT') NOT NULL DEFAULT 'PRODUCTION';
--rollback alter table workflow MODIFY role enum('PRODUCTION','PVS_TRAIN','PVS_EXEC','EXPERIMENTATION') NOT NULL DEFAULT 'PRODUCTION';

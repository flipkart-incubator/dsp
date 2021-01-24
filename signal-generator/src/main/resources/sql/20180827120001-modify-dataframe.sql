--liquibase formatted sql

--changeset rashmi.gulhane:1
ALTER TABLE `dataframe_audit` add `dashboard_title` varchar(255) AFTER `job_id`;
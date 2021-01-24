--liquibase formatted sql

--changeset ravikiran.kalal:1
ALTER TABLE `request` ADD `response` TEXT  NULL;

--changeset ravikiran.kalal:2
ALTER TABLE `request` ADD `varadhi_response` TEXT  NULL;


--liquibase formatted sql

--changeset rashmi.gulhane:1
ALTER TABLE `execution_environment` ADD `docker_hub` varchar(255);

--changeset rashmi.gulhane:2
ALTER TABLE `execution_environment` ADD `image_identifier` varchar(255);

--changeset rashmi.gulhane:3
ALTER TABLE `execution_environment` ADD `image_version` varchar(255);

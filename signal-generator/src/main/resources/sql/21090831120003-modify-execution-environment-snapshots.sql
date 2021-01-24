--liquibase formatted sql

--changeset rashmi.gulhane:1
ALTER TABLE `execution_environment_snapshots` ADD `native_library_set` TEXT;

--changeset rashmi.gulhane:2
ALTER TABLE `execution_environment_snapshots` ADD `os` varchar(255);

--changeset rashmi.gulhane:3
ALTER TABLE `execution_environment_snapshots` ADD `os_version` varchar(255);

--changeset rashmi.gulhane:4
ALTER TABLE `execution_environment_snapshots` ADD `language_version` varchar(255);

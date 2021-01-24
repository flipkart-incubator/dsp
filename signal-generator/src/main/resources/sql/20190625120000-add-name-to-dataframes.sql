--liquibase formatted sql

--changeset ravikiran.kalal:1
ALTER TABLE `dataframes` ADD `name` VARCHAR(50)  NOT NULL  DEFAULT ''  AFTER `id`;

ALTER TABLE `dataframe_audit` DROP FOREIGN KEY `dataframe_audit_dataframe_id_constraint`;

ALTER TABLE `dataframe_audit` DROP FOREIGN KEY `FKfj126n43qrctqd90956snqnb7`;

ALTER TABLE `workflow_to_dataframes` DROP FOREIGN KEY `dataframe_key`;

ALTER TABLE `dataframes` CHANGE `id` `id` INT(20)  NOT NULL  COMMENT 'Unique identifier.';

/* 1:34:36 AM Localhost sg_test */ ALTER TABLE `dataframes` DROP PRIMARY KEY;
/* 1:34:36 AM Localhost sg_test */ ALTER TABLE `dataframes` ADD PRIMARY KEY (`id`);

/* 3:45:00 PM Localhost sg_test */ ALTER TABLE `dataframe_audit` CHANGE `dataframe_id` `dataframe_id` INT(20)  NOT NULL  COMMENT 'Description of the fact.';
/* 1:34:57 AM Localhost sg_test */ ALTER TABLE `dataframe_audit` ADD FOREIGN KEY (`dataframe_id`) REFERENCES `dataframes` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

/* 3:45:29 PM Localhost sg_test */ ALTER TABLE `workflow_to_dataframes` CHANGE `dataframe_id` `dataframe_id` INT(20)  NOT NULL  COMMENT 'Unique identifier for dataframe';
/* 3:45:59 PM Localhost sg_test */ ALTER TABLE `workflow_to_dataframes` ADD FOREIGN KEY (`dataframe_id`) REFERENCES `dataframes` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;


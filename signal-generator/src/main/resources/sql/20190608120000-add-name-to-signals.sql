--liquibase formatted sql

--changeset ravikiran.kalal:1
ALTER TABLE `signals`
    ADD `name` VARCHAR(100) NULL DEFAULT NULL AFTER `id`;

--changeset ravikiran.kalal:2
ALTER TABLE `signal_groups_to_signals`
    DROP FOREIGN KEY `signal_constraint`;

--changeset ravikiran.kalal:3
ALTER TABLE `signal_groups_to_signals`
    DROP FOREIGN KEY `FK4r6cvmf679b3dt83vo7tp31rb`;

--changeset ravikiran.kalal:4
ALTER TABLE `signals`
    CHANGE `id` `id` INT(50) NOT NULL COMMENT 'Unique identifier.';

--changeset ravikiran.kalal:5
ALTER TABLE `signal_groups_to_signals`
    DROP `signal_id`;

--changeset ravikiran.kalal:6
ALTER TABLE `signal_groups_to_signals`
    ADD `signal_id` INT(200) NULL DEFAULT NULL AFTER `signal_group_id`;

--changeset ravikiran.kalal:7
ALTER TABLE `signal_groups_to_signals`
    ADD FOREIGN KEY (`signal_id`) REFERENCES `signals` (`id`);


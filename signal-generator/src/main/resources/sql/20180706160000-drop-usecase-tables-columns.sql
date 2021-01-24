--liquibase formatted sql

--changeset srikanth.vuppuluri:1
DROP TABLE data_tables_to_sg_usecase;
--rollback CREATE TABLE `data_tables_to_sg_usecase` (
--   `table_id` varchar(50) NOT NULL COMMENT 'Table id.',
--   `sg_usecase` varchar(50) NOT NULL COMMENT 'Usecase',
--   `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which tables_to_facts_or_dim was created.',
--   `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which tables_to_facts_or_level was updated.',
--   KEY `FKk5p6yiw6gjhbpnd80sbw35a9` (`table_id`),
--   KEY `FKjeetqe06p0u6051tfc0saxv1b` (`sg_usecase`),
--   KEY `i1` (`sg_usecase`),
--   KEY `i2` (`table_id`),
--   KEY `i3` (`table_id`,`sg_usecase`),
--   CONSTRAINT `FKjeetqe06p0u6051tfc0saxv1b` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases` (`sg_usecase`),
--   CONSTRAINT `FKk5p6yiw6gjhbpnd80sbw35a9` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`),
--   CONSTRAINT `sg_usecase_constraint` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases` (`sg_usecase`) ON DELETE CASCADE ON UPDATE CASCADE,
--   CONSTRAINT `tables_constraint` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the tables_to_facts_or_dim maintained in the system.';

--changeset srikanth.vuppuluri:2
ALTER TABLE workflow DROP FOREIGN KEY FKodroh8xfagth50osb7lkh83fr, DROP FOREIGN KEY sg_usecase_workflow_constraint, DROP COLUMN sg_usecase;
--rollback ALTER TABLE workflow ADD COLUMN sg_usecase varchar(50);

--liquibase formatted sql

--changeset srikanth.vuppuluri:1
ALTER TABLE signal_groups_to_signals ADD COLUMN data_table_id VARCHAR(50);
--rollback ALTER TABLE signal_groups_to_signals DROP COLUMN data_table_id;

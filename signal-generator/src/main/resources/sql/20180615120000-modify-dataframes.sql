--liquibase formatted sql

--changeset srikanth.vuppuluri:1
ALTER TABLE dataframes
DROP FOREIGN KEY FK4g2gd76d6m46t21lhx8rkovty,
DROP FOREIGN KEY dataframes_sg_usecase_constraint,
DROP COLUMN partition_keys,
DROP COLUMN sg_usecase;
--rollback ALTER TABLE dataframes ADD COLUMN partition_keys text, ADD COLUMN sg_usecase varchar(50) NOT NULL;
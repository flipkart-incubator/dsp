--liquibase formatted sql

--changeset srikanth.vuppuluri:1
ALTER TABLE script CHANGE ts created_at timestamp NULL DEFAULT NULL;
--rollback ALTER TABLE script CHANGE created_at ts datetime DEFAULT NULL;

--changeset srikanth.vuppuluri:2
ALTER TABLE script ADD COLUMN updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 ADD COLUMN is_draft TINYINT (1) DEFAULT 0 ,ADD COLUMN version DECIMAL(5, 2) DEFAULT 1;
--rollback ALTER TABLE script DROP COLUMN updated_at, DROP COLUMN is_draft, DROP COLUMN version;

--changeset srikanth.vuppuluri:3
ALTER TABLE script DROP INDEX UKrrhorptuyyn242ugq11plvilf, ADD UNIQUE KEY uniquescript (git_file_path,git_commit_id,git_folder,git_repo,is_draft,version)
--rollback ALTER TABLE script DROP INDEX uniquescript, ADD UNIQUE KEY UKrrhorptuyyn242ugq11plvilf (git_file_path,git_commit_id,git_folder,git_repo);
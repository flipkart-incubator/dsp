--liquibase formatted sql

--changeset srikanth.vuppuluri:1
ALTER TABLE request_step DROP FOREIGN KEY `FKbhf6f87rvrb055718lx1lxetw`;
ALTER TABLE request MODIFY COLUMN id bigint(20) NOT NULL AUTO_INCREMENT;
ALTER TABLE request_step ADD CONSTRAINT `FKbhf6f87rvrb055718lx1lxetw` FOREIGN KEY (request_id) REFERENCES request (id) ON DELETE CASCADE;

--changeset srikanth.vuppuluri:2
ALTER TABLE request_step_audit DROP FOREIGN KEY `FKc40f7xf21sl6imtgolyb2lubn`;
ALTER TABLE request_step MODIFY COLUMN id bigint(20) NOT NULL AUTO_INCREMENT;
ALTER TABLE request_step_audit ADD CONSTRAINT `FKc40f7xf21sl6imtgolyb2lubn` FOREIGN KEY (`request_step_id`) REFERENCES `request_step` (`id`) ON DELETE CASCADE

--changeset srikanth.vuppuluri:3
ALTER TABLE request_step_audit MODIFY COLUMN id bigint(20) NOT NULL AUTO_INCREMENT;

--changeset srikanth.vuppuluri:4
ALTER TABLE pipeline_step_audits MODIFY COLUMN id bigint(20) NOT NULL AUTO_INCREMENT;

--changeset srikanth.vuppuluri:5
ALTER TABLE workflow_audits MODIFY COLUMN id bigint(20) NOT NULL AUTO_INCREMENT;

--changeset srikanth.vuppuluri:6
ALTER TABLE pipeline_step_runtime_config MODIFY COLUMN id bigint(20) NOT NULL AUTO_INCREMENT;

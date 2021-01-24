--liquibase formatted sql

--changeset ketan.patil:1
ALTER TABLE `pipeline_step_audits`
    ADD `attempt` int(11) NOT NULL DEFAULT '0';

--changeset ketan.patil:2
ALTER TABLE `pipeline_step_audits`
    MODIFY `logs` varchar(1000) DEFAULT NULL;

--changeset ketan.patil:3
ALTER TABLE `pipeline_step_audits`
    DROP INDEX  `UKbr8orlai43a6tnyn1bhjcrvnq`;

--changeset ketan.patil:4
ALTER TABLE `pipeline_step_audits`
    ADD CONSTRAINT `unique_peid_psid_atmp` UNIQUE (`pipeline_execution_id`,`pipeline_step_id`,`attempt`);
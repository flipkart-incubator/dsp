--liquibase formatted sql

--changeset srikanth.vuppuluri:1
alter table pipeline drop column input_granularity_id, drop column output_granularity_id,
 drop column signal_group_id, MODIFY criteria VARCHAR(255);

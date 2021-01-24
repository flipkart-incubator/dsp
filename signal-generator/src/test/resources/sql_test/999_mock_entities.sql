--liquibase formatted sql
--changeset runOnChange:true endDelimiter:#

INSERT INTO `signals` (`id`, `name`, `data_type`, `ds_type`, `signal_definition`, `base_entity`, `created_at`, `updated_at`)
VALUES
    (4, 'signal_3_test', 'BIG_INTEGER', 'REGRESSION', '{\"value_type\":\"ONE_TO_ONE\",\"default_value\":0,\"aggregation_type\":null,\"group_by\":null,\"scopes\":null}', 'batch', '2018-05-08 12:23:25', '2018-05-08 14:07:40');#

INSERT INTO `data_sources` (`id`, `configuration`, `created_at`, `updated_at`) VALUES ('dcp_test', '{\"type\":\"HIVE\",\"host_ip\":\"0.0.0.0\",\"database\":\"dcp_fact\"}', '2017-03-17 17:16:05', '2017-03-17 17:16:05');#

INSERT INTO `data_tables` (`id`, `description`, `type`, `data_source_id`, `config`, `created_at`, `updated_at`) VALUES ('dimension_test', NULL, 'DIM_TABLE', 'dcp_test', NULL, '2018-05-31 10:52:07', '2018-05-31 10:52:07');#

INSERT INTO `data_subscriptions` (`id`, `description`, `configuration`, `created_at`, `updated_at`) VALUES ('test', NULL, NULL, NULL, '2017-01-05 02:54:21');#

INSERT INTO `signal_groups` (`id`, `description`, `created_at`, `updated_at`) VALUES ('test_signal_group', NULL, '2018-05-13 01:40:19', '2018-05-13 01:40:19');#

INSERT INTO `dataframes` (`id`, `name`, `signal_group_id`, `config`, `created_at`, `updated_at`) VALUES (1,'test_dataframe', 'test_signal_group', '{\"dataframe_scope\":[{\"predicate_entity\":{\"name\":\"week\"},\"predicate\":{\"type\":\"biValue\",\"predicate_type\":\"INCREMENTAL_WEEK_RANGE\",\"value1\":\"201602\",\"value2\":\"-10\"}}],\"visible_signals\":[{\"name\":\"signal_1_test\"}]}', '2018-05-13 01:41:03', '2018-05-14 23:18:03');#

INSERT INTO `signals` (`id`, `name`, `data_type`, `ds_type`, `signal_definition`, `base_entity`, `created_at`, `updated_at`) VALUES (1, 'signal_1_test', 'TEXT', 'CLASSIFICATION', '{\"value_type\":\"ONE_TO_ONE\",\"default_value\":0,\"aggregation_type\":null,\"group_by\":null,\"scopes\":null}', 'fsn', '2018-05-08 12:10:00', '2018-05-08 13:20:40');#
INSERT INTO `signals` (`id`, `name`, `data_type`, `ds_type`, `signal_definition`, `base_entity`, `created_at`, `updated_at`) VALUES (2, 'signal_2_test', 'TEXT', 'CLASSIFICATION', '{\"value_type\":\"ONE_TO_ONE\",\"default_value\":0,\"aggregation_type\":null,\"group_by\":null,\"scopes\":null}', 'vertical', '2018-05-08 15:18:23', '2018-05-31 15:43:29');#
INSERT INTO `signals` (`id`, `name`, `data_type`, `ds_type`, `signal_definition`, `base_entity`, `created_at`, `updated_at`) VALUES (3, 'week', 'TEXT', 'CLASSIFICATION', '{\"value_type\":\"ONE_TO_ONE\",\"default_value\":0,\"aggregation_type\":null,\"group_by\":null,\"scopes\":null}', 'week', '2018-05-08 15:18:23', '2018-05-31 15:43:29');#


INSERT INTO `signal_groups_to_signals` (`signal_group_id`, `signal_id`, `is_primary`, `created_at`, `updated_at`,`data_table_id`) VALUES ('test_signal_group', 1, '1', '2018-05-08 15:21:40', '2018-05-08 15:21:46','dimension_test');#

INSERT INTO `signal_groups_to_signals` (`signal_group_id`, `signal_id`, `is_primary`, `created_at`, `updated_at`,`data_table_id`) VALUES ('test_signal_group', 2, '0', '2018-05-08 15:22:14', '2018-05-08 15:22:18','dimension_test');#
INSERT INTO `signal_groups_to_signals` (`signal_group_id`, `signal_id`, `is_primary`, `created_at`, `updated_at`,`data_table_id`) VALUES ('test_signal_group', 3, '0', '2018-05-08 15:22:35', '2018-05-08 15:22:40','dimension_test');#


INSERT INTO `dataframe_audit` (`run_id`, `dataframe_id`, `status`, `payload`, `config`, `created_at`, `updated_at`, `job_id`) VALUES ('27', 1, 'COMPLETED', '{\n  \"request_id\": 7377940,\n  \"data_frame_id\": \"fsn_ml_train_dataframe\",\n  \"column_metadata\": {\n    \"week\": \"RANGE\"\n  },\n  \"dataframes\": {\n    \"[{\\\"type\\\":\\\"biValue\\\",\\\"column_type\\\":\\\"RANGE\\\",\\\"first_value\\\":\\\"2016-01-06\\\",\\\"second_value\\\":\\\"2018-05-14\\\"}]\": [\n      \"hdfs://hadoopcluster2/projects/planning/dsp_sg.db/fsn_ml_train_usecase__25/vertical=0/000000_0\"\n    ]\n  }\n}', '{\n  \"dataframe_scope\": [\n    {\n      \"predicate_entity\": {\n        \"name\": \"week\"\n      },\n      \"predicate\": {\n        \"type\": \"biValue\",\n        \"predicate_type\": \"INCREMENTAL_WEEK_RANGE\",\n        \"value1\": \"201602\",\n        \"value2\": \"-10\"\n      }\n    }\n  ],\n  \"visible_signals\": [\n    {\n      \"name\": \"signal_1_test\"\n    }\n  ]\n}', '2018-05-15 13:54:36', '2018-05-15 15:46:06', '7377940_401985915121794');#
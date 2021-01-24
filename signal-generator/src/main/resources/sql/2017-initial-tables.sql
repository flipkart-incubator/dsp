--liquibase formatted sql

--changeset manu.m:1

CREATE TABLE `callback_info` (
  `id` bigint(20) NOT NULL,
  `data` longtext,
  `refresh_id` bigint(20) NOT NULL,
  `signal_groups` longtext,
  `workflows` longtext,
  `status` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  `run_type` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKrufydcon1oscdmrhcnj38u0p9` (`refresh_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `collate` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `collate_table_name` varchar(255) DEFAULT NULL,
  `node_name` varchar(255) DEFAULT NULL,
  `partition_scope` longtext,
  `meta` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `data_sources` (
  `id` varchar(50) NOT NULL COMMENT 'Unique identifier.',
  `configuration` text NOT NULL COMMENT 'Configuration of the data source.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which data source was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which data source was updated.',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the data source maintained in the system.';

CREATE TABLE `data_tables` (
  `id` varchar(50) NOT NULL COMMENT 'Unique identifier.',
  `description` text COMMENT 'Description of the table.',
  `type` enum('FACT_TABLE','DIM_TABLE') NOT NULL COMMENT 'Table type which can be either FACT_TABLE or DIMENSION_TABLE',
  `data_source_id` varchar(50) NOT NULL COMMENT 'Points to the database where the table is available',
  `config` text COMMENT 'Configuration for the table.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which data source was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which data source was updated.',
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK8l1ud5hqk607xknfowhg3jfli` (`id`,`data_source_id`),
  KEY `FKix2mtmmr3rvgcv0c3jmgorpmi` (`data_source_id`),
  CONSTRAINT `FKix2mtmmr3rvgcv0c3jmgorpmi` FOREIGN KEY (`data_source_id`) REFERENCES `data_sources` (`id`),
  CONSTRAINT `tables_data_source_constraint` FOREIGN KEY (`data_source_id`) REFERENCES `data_sources` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the data source maintained in the system.';

CREATE TABLE `data_subscriptions` (
  `id` varchar(50) NOT NULL COMMENT 'Unique identifier.',
  `description` text COMMENT 'Description of the fact.',
  `configuration` text COMMENT 'Data subscription configuration.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which data subscription was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which data subscription was updated.',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the data subscription maintained in the system.';

CREATE TABLE `data_subscriptions_to_data_tables` (
  `subscription_id` varchar(50) NOT NULL COMMENT 'Data subscription unique identifier.',
  `table_id` varchar(50) NOT NULL COMMENT 'Id of the table that got refreshed.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which fact was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which fact was updated.',
  KEY `FKsl2q7qhtq9gra7ua4vvbwbixm` (`table_id`),
  KEY `FK791elhncj6b84mecdoh8q94gb` (`subscription_id`),
  CONSTRAINT `data_subscriptions_to_tables_subscription_id_constraint` FOREIGN KEY (`subscription_id`) REFERENCES `data_subscriptions` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `data_subscriptions_to_tables_table_id_constraint` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK791elhncj6b84mecdoh8q94gb` FOREIGN KEY (`subscription_id`) REFERENCES `data_subscriptions` (`id`),
  CONSTRAINT `FKsl2q7qhtq9gra7ua4vvbwbixm` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the facts maintained in the system.';

CREATE TABLE `data_table_refresh` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier.',
  `request_id` bigint(11) NOT NULL COMMENT 'Request id',
  `table_id` varchar(50) NOT NULL COMMENT 'Table that got refreshed.',
  `table_refresh_id` int(11) NOT NULL COMMENT 'Refresh ID of the table that got refreshed.',
  `subscription_id` varchar(100) NOT NULL COMMENT 'Subscription id.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which table refresh was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which table refresh was updated.',
  PRIMARY KEY (`id`),
  KEY `FK4fnalh0jxtk48f5kg65sohet3` (`subscription_id`),
  KEY `FKkr1lakcct7yonb05a36859834` (`table_id`),
  CONSTRAINT `FK4fnalh0jxtk48f5kg65sohet3` FOREIGN KEY (`subscription_id`) REFERENCES `data_subscriptions` (`id`),
  CONSTRAINT `FKkr1lakcct7yonb05a36859834` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`),
  CONSTRAINT `tables_refresh_subscription_id_constraint` FOREIGN KEY (`subscription_id`) REFERENCES `data_subscriptions` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `table_refresh_request_id_constraint` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the table refresh maintained in the system.';

CREATE TABLE `data_tables_to_facts_or_levels` (
  `table_id` varchar(50) NOT NULL COMMENT 'Table id.',
  `type` enum('FACT','LEVEL') NOT NULL COMMENT 'Table type which can be either FACT table or LEVEL table.',
  `fact_or_level_id` varchar(50) NOT NULL COMMENT 'Fact or level id',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which tables_to_facts_or_dim was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which tables_to_facts_or_level was updated.',
  KEY `FKiovk4kn1ok7fi8cmxis2hvsod` (`table_id`),
  KEY `i1` (`table_id`,`fact_or_level_id`),
  KEY `i2` (`table_id`),
  KEY `i3` (`fact_or_level_id`),
  KEY `i4` (`type`,`fact_or_level_id`),
  CONSTRAINT `FKiovk4kn1ok7fi8cmxis2hvsod` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the tables_to_facts_or_dim maintained in the system.';

CREATE TABLE `sg_usecases` (
  `sg_usecase` varchar(50) NOT NULL COMMENT 'Unique identifier for level.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which dimension was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which dimension was updated.',
  `partition_keys` text,
  PRIMARY KEY (`sg_usecase`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the dimension maintained in the system.';

CREATE TABLE `signal_groups` (
  `id` varchar(50) NOT NULL COMMENT 'Unique identifier.',
  `description` text COMMENT 'Description of the signal group.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which signal group was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which signal group was updated.',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the signal group maintained in the system.';

CREATE TABLE `data_tables_to_sg_usecase` (
  `table_id` varchar(50) NOT NULL COMMENT 'Table id.',
  `sg_usecase` varchar(50) NOT NULL COMMENT 'Usecase',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which tables_to_facts_or_dim was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which tables_to_facts_or_level was updated.',
  KEY `FKk5p6yiw6gjhbpnd80sbw35a9` (`table_id`),
  KEY `FKjeetqe06p0u6051tfc0saxv1b` (`sg_usecase`),
  KEY `i1` (`sg_usecase`),
  KEY `i2` (`table_id`),
  KEY `i3` (`table_id`,`sg_usecase`),
  CONSTRAINT `FKjeetqe06p0u6051tfc0saxv1b` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases` (`sg_usecase`),
  CONSTRAINT `FKk5p6yiw6gjhbpnd80sbw35a9` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`),
  CONSTRAINT `sg_usecase_constraint` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases` (`sg_usecase`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `tables_constraint` FOREIGN KEY (`table_id`) REFERENCES `data_tables` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the tables_to_facts_or_dim maintained in the system.';

CREATE TABLE `dataframes` (
  `id` varchar(50) NOT NULL COMMENT 'Unique identifier.',
  `signal_group_id` varchar(50) NOT NULL COMMENT 'Signal group id defining the dataframe.',
  `partition_keys` text COMMENT 'Partition keys.',
  `config` text COMMENT 'Where clause that defines this dataframe.',
  `sg_usecase` varchar(50) NOT NULL COMMENT 'Usecase that use the dataframe.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which dataframes was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which dataframes was updated.',
  `sg_type` varchar(255) DEFAULT NULL,
  `dcp_table_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK4g2gd76d6m46t21lhx8rkovty` (`sg_usecase`),
  KEY `FKe73gl1owu7w7qkv0n96geu46q` (`signal_group_id`),
  KEY `i1` (`sg_usecase`),
  CONSTRAINT `dataframes_sg_usecase_constraint` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases` (`sg_usecase`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `dataframes_signal_group_id_constraint` FOREIGN KEY (`signal_group_id`) REFERENCES `signal_groups` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK4g2gd76d6m46t21lhx8rkovty` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases` (`sg_usecase`),
  CONSTRAINT `FKe73gl1owu7w7qkv0n96geu46q` FOREIGN KEY (`signal_group_id`) REFERENCES `signal_groups` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `dataframe_audit` (
  `run_id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier.',
  `dataframe_id` varchar(50) NOT NULL COMMENT 'Description of the fact.',
  `status` enum('ENQUEUED','GENERATING_GRANULARITY_AND_FACT_TABLES','GENERATING_DATAFRAME','GENERATING_PAYLOAD','COMPLETED','FAILED') NOT NULL COMMENT 'Status of the dataframe.',
  `payload` longtext COMMENT 'path where the dataframe is located.',
  `request_id` int(11) NOT NULL COMMENT 'Request id',
  `config` text COMMENT 'Absolute scope for the dataframe audit run',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which dataframe_audit was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which dataframe_audit was updated.',
  `job_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`run_id`),
  KEY `FKfj126n43qrctqd90956snqnb7` (`dataframe_id`),
  KEY `request_id` (`request_id`),
  CONSTRAINT `dataframe_audit_dataframe_id_constraint` FOREIGN KEY (`dataframe_id`) REFERENCES `dataframes` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FKfj126n43qrctqd90956snqnb7` FOREIGN KEY (`dataframe_id`) REFERENCES `dataframes` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the dataframe_audit maintained in the system.';

CREATE TABLE `execution_environment` (
  `id` bigint(20) NOT NULL,
  `execution_env` varchar(255) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `image_path` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `startup_script_path` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKoql60pq564ukjoyu1kcbwq09o` (`execution_env`),
  KEY `execution_env` (`execution_env`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `facts` (
  `id` varchar(50) NOT NULL COMMENT 'Unique identifier.',
  `description` text COMMENT 'Description of the fact.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which fact was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which fact was updated.',
  PRIMARY KEY (`id`),
  KEY `i1` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the facts maintained in the system.';

CREATE TABLE `levels` (
  `id` varchar(50) NOT NULL COMMENT 'Unique identifier for level.',
  `dimension` varchar(50) NOT NULL COMMENT 'Unique identifier for dimension.',
  `child` varchar(50) DEFAULT NULL COMMENT 'Child level',
  `description` text COMMENT 'Description of the level.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which dimension was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which dimension was updated.',
  PRIMARY KEY (`id`),
  KEY `i1` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the dimension maintained in the system.';

CREATE TABLE `workflow` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `description` varchar(255) DEFAULT NULL,
  `name` varchar(255) NOT NULL,
  `parent_workflow_id` bigint(20) DEFAULT NULL,
  `sg_usecase` varchar(50) DEFAULT NULL,
  `dependent_sg_workflows` varchar(255) DEFAULT NULL,
  `workflow_execution_type` enum('ML_TRAIN','ML_EXECUTE','TRAIN_AND_EXECUTE') NOT NULL,
  `execution_cluster` varchar(255) NOT NULL,
  `is_preemptable` tinyint(1) DEFAULT NULL,
  `retries` int(11) NOT NULL DEFAULT '3',
  `role` enum('PRODUCTION','PVS_TRAIN','PVS_EXEC') NOT NULL DEFAULT 'PRODUCTION',
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK3je18ux0wru0pxv6un40yhbn4` (`name`),
  KEY `FKjlj4em3c50t7rtoxqljmri5c3` (`parent_workflow_id`),
  KEY `FKodroh8xfagth50osb7lkh83fr` (`sg_usecase`),
  CONSTRAINT `FKjlj4em3c50t7rtoxqljmri5c3` FOREIGN KEY (`parent_workflow_id`) REFERENCES `workflow` (`id`),
  CONSTRAINT `FKodroh8xfagth50osb7lkh83fr` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases` (`sg_usecase`),
  CONSTRAINT `sg_usecase_workflow_constraint` FOREIGN KEY (`sg_usecase`) REFERENCES `sg_usecases` (`sg_usecase`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `workflow_audits` (
  `id` bigint(20) NOT NULL,
  `pipeline_execution_id` varchar(255) NOT NULL,
  `pipeline_id` bigint(20) NOT NULL,
  `refresh_id` bigint(20) NOT NULL,
  `scope` longtext,
  `status` varchar(255) DEFAULT NULL,
  `workflow_execution_id` varchar(255) NOT NULL,
  `workflow_id` bigint(20) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKkls83i52ccti6oyp43qwml9vq` (`refresh_id`,`workflow_id`,`workflow_execution_id`,`pipeline_execution_id`),
  KEY `FK43nxg30pgiolqhtigm4mij3n9` (`workflow_id`),
  KEY `pipeline_execution_id` (`pipeline_execution_id`),
  CONSTRAINT `FK43nxg30pgiolqhtigm4mij3n9` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `workflow_group_meta` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `azkaban_job_name` varchar(255) DEFAULT NULL,
  `mandatory_fields` longtext,
  `callback_entities` longtext,
  `callback_url` text,
  `kill_time_for_notification` bigint(20) DEFAULT NULL,
  `warning_time_for_notification` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `workflow_group` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `description` varchar(255) DEFAULT NULL,
  `name` varchar(255) NOT NULL,
  `sg_usecases` varchar(255) DEFAULT NULL,
  `subscription_id` varchar(255) DEFAULT NULL,
  `wfg_meta_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKlrknunl8jdxo6daq3h2uluqg7` (`name`),
  KEY `FKrkguupuqf9h8vls6lw5mg5s7g` (`wfg_meta_id`),
  CONSTRAINT `FKrkguupuqf9h8vls6lw5mg5s7g` FOREIGN KEY (`wfg_meta_id`) REFERENCES `workflow_group_meta` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `workflow_meta` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `meta` longtext,
  `ts` datetime DEFAULT NULL,
  `workflow_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `FKj4txrtm2712u2w018p8v26q94` (`workflow_id`),
  CONSTRAINT `FKj4txrtm2712u2w018p8v26q94` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `workflowGroups_to_workflows` (
  `workflowGroup_id` bigint(20) NOT NULL,
  `workflow_id` bigint(20) NOT NULL,
  KEY `FKhsal7q5ei604l3q205uxtvqxs` (`workflow_id`),
  KEY `FK89eb7idjv0r7cgl546ur9hw` (`workflowGroup_id`),
  CONSTRAINT `FK89eb7idjv0r7cgl546ur9hw` FOREIGN KEY (`workflowGroup_id`) REFERENCES `workflow_group` (`id`),
  CONSTRAINT `FKhsal7q5ei604l3q205uxtvqxs` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `notification_preference` (
  `id` bigint(20) NOT NULL,
  `notification_preference_type` varchar(255) NOT NULL,
  `recipients_email` longtext,
  `workflow_group_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKf0pyhsuhlgrqnqjyt8j0oa7fr` (`notification_preference_type`,`workflow_group_id`),
  KEY `FKpsc0yq8lohvpqck0c0fq4av6j` (`workflow_group_id`),
  CONSTRAINT `FKpsc0yq8lohvpqck0c0fq4av6j` FOREIGN KEY (`workflow_group_id`) REFERENCES `workflow_group` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `pipeline` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `criteria` varchar(255) NOT NULL,
  `description` varchar(255) NOT NULL,
  `input_granularity_id` varchar(255) NOT NULL,
  `output_granularity_id` varchar(255) NOT NULL,
  `signal_group_id` bigint(20) NOT NULL,
  `workflow_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `FKrm8vdu2bdbk7c05xwr9pub365` (`workflow_id`),
  CONSTRAINT `FKrm8vdu2bdbk7c05xwr9pub365` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `script` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `execution_env` varchar(255) NOT NULL,
  `git_commit_id` varchar(255) NOT NULL,
  `git_file_path` varchar(255) NOT NULL,
  `git_repo` varchar(255) NOT NULL,
  `input_variables` text,
  `metadata` varchar(255) DEFAULT NULL,
  `output_variables` text,
  `ts` datetime DEFAULT NULL,
  `git_folder` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKrrhorptuyyn242ugq11plvilf` (`git_file_path`,`git_commit_id`,`git_folder`,`git_repo`),
  KEY `FKdofurh1me2uh0yinncyhold2e` (`execution_env`),
  CONSTRAINT `execution_env_cons` FOREIGN KEY (`execution_env`) REFERENCES `execution_environment` (`execution_env`),
  CONSTRAINT `FKdofurh1me2uh0yinncyhold2e` FOREIGN KEY (`execution_env`) REFERENCES `execution_environment` (`execution_env`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `pipeline_step` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `pipeline_step_config` varchar(255) DEFAULT NULL,
  `pipeline_step_type` varchar(255) NOT NULL,
  `pipeline_id` bigint(20) NOT NULL,
  `script_id` bigint(20) DEFAULT NULL,
  `pipeline_step_resources` varchar(225) DEFAULT NULL,
  `parent_pipeline_step_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK55vfpdse9xepotvuiq1cbucw7` (`pipeline_id`),
  KEY `FKdaglwf7bahnid2t2f6w68qn8y` (`script_id`),
  KEY `FK49q24jjo5gmwo63rqvaua7e94` (`parent_pipeline_step_id`),
  CONSTRAINT `FK49q24jjo5gmwo63rqvaua7e94` FOREIGN KEY (`parent_pipeline_step_id`) REFERENCES `pipeline_step` (`id`),
  CONSTRAINT `FK55vfpdse9xepotvuiq1cbucw7` FOREIGN KEY (`pipeline_id`) REFERENCES `pipeline` (`id`),
  CONSTRAINT `FKdaglwf7bahnid2t2f6w68qn8y` FOREIGN KEY (`script_id`) REFERENCES `script` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `pipeline_step_audits` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime NOT NULL,
  `pipeline_execution_id` varchar(255) NOT NULL,
  `pipeline_step_id` bigint(20) NOT NULL,
  `refresh_id` bigint(20) NOT NULL,
  `status` varchar(255) DEFAULT NULL,
  `updated_at` datetime NOT NULL,
  `workflow_execution_id` varchar(255) NOT NULL,
  `resources` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKbr8orlai43a6tnyn1bhjcrvnq` (`pipeline_execution_id`,`pipeline_step_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `pipeline_step_runtime_config` (
  `id` bigint(20) NOT NULL,
  `pipeline_execution_id` varchar(255) NOT NULL,
  `run_config` longtext,
  `scope` longtext NOT NULL,
  `ts` datetime DEFAULT NULL,
  `workflow_execution_id` varchar(255) NOT NULL,
  `pipeline_step_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKq0mv6wc25ifs0nqffx3dbpaly` (`pipeline_execution_id`,`pipeline_step_id`),
  KEY `FKc1s25y0jls0swt1hv2mdfn852` (`pipeline_step_id`),
  CONSTRAINT `FKc1s25y0jls0swt1hv2mdfn852` FOREIGN KEY (`pipeline_step_id`) REFERENCES `pipeline_step` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `request` (
  `id` bigint(20) NOT NULL,
  `azkaban_exec_id` bigint(20) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `data` longtext NOT NULL,
  `request_id` bigint(20) DEFAULT NULL,
  `request_status` varchar(255) DEFAULT NULL,
  `updated_at` datetime NOT NULL,
  `workflow_group_id` bigint(20) NOT NULL,
  `is_notified` bit(1) DEFAULT NULL,
  `workflow_details_snapshot` longtext,
  `callback_url` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `request_step` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime NOT NULL,
  `metaData` longtext,
  `step_type` varchar(255) NOT NULL,
  `updated_at` datetime NOT NULL,
  `request_id` bigint(20) NOT NULL,
  `job_name` longtext,
  PRIMARY KEY (`id`),
  KEY `FKbhf6f87rvrb055718lx1lxetw` (`request_id`),
  CONSTRAINT `FKbhf6f87rvrb055718lx1lxetw` FOREIGN KEY (`request_id`) REFERENCES `request` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `request_step_audit` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime NOT NULL,
  `meta_data` longtext,
  `status` varchar(255) DEFAULT NULL,
  `updated_at` datetime NOT NULL,
  `request_step_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `FKc40f7xf21sl6imtgolyb2lubn` (`request_step_id`),
  CONSTRAINT `FKc40f7xf21sl6imtgolyb2lubn` FOREIGN KEY (`request_step_id`) REFERENCES `request_step` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `secondary_config_payload` (
  `id` bigint(20) NOT NULL,
  `num_of_algos` int(11) NOT NULL,
  `pipeline_execution_id` varchar(255) NOT NULL,
  `secondary_config_payload` longtext NOT NULL,
  `status` varchar(255) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKc24hj1wu8g41tehgy3o9sv9g4` (`pipeline_execution_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `signals` (
  `id` varchar(50) NOT NULL COMMENT 'Unique identifier.',
  `data_type` enum('TEXT','DOUBLE','FLOAT','INTEGER','BIG_INTEGER','TIME_DAY','TIME_WEEK','TIME_MONTH','TIME_YEAR') NOT NULL COMMENT 'Data type of the signals as represented in hive.',
  `ds_type` enum('CLASSIFICATION','REGRESSION') NOT NULL COMMENT 'Type of signal which can be either CLASSIFICATION OR REGRESSION',
  `signal_definition` text NOT NULL COMMENT 'Signal definition',
  `base_entity` varchar(50) NOT NULL COMMENT 'Underlying table from which signal can be derived.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which signals was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which signals was updated.',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the signals maintained in the system.';

CREATE TABLE `signal_groups_to_signals` (
  `signal_group_id` varchar(50) NOT NULL COMMENT 'Signal group identifier.',
  `signal_id` varchar(50) NOT NULL COMMENT 'Unique signal identifier.',
  `is_primary` tinyint(1) NOT NULL COMMENT 'Is this signal part of the unique key for the signal group.',
  `created_at` timestamp NULL DEFAULT NULL COMMENT 'Time at which signal_groups_to_signals was created.',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Time at which signal_groups_to_signals was updated.',
  KEY `FKt8cuwb2addloqmtin7sy886on` (`signal_group_id`),
  KEY `FK4r6cvmf679b3dt83vo7tp31rb` (`signal_id`),
  CONSTRAINT `FK4r6cvmf679b3dt83vo7tp31rb` FOREIGN KEY (`signal_id`) REFERENCES `signals` (`id`),
  CONSTRAINT `FKt8cuwb2addloqmtin7sy886on` FOREIGN KEY (`signal_group_id`) REFERENCES `signal_groups` (`id`),
  CONSTRAINT `signal_constraint` FOREIGN KEY (`signal_id`) REFERENCES `signals` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `signal_group_constraint` FOREIGN KEY (`signal_group_id`) REFERENCES `signal_groups` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains all the signal_groups_to_signals maintained in the system.';

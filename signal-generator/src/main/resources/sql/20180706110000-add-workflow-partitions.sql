--liquibase formatted sql

--changeset srikanth.vuppuluri:1
CREATE TABLE workflow_partitions (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  workflow_id bigint(20) DEFAULT NULL,
  input_partition varchar(45) NOT NULL,
  output_partition varchar(45) NOT NULL,
  created_at timestamp NULL DEFAULT NULL,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY wp_workflow_id (workflow_id),
  CONSTRAINT `wp_workflow_id` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;
--rollback DROP TABLE workflow_partitions;

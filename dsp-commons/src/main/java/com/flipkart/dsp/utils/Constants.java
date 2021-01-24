package com.flipkart.dsp.utils;

import com.fasterxml.jackson.databind.JavaType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class Constants {
    //Execution Environment
    public static final String IMAGE_PATH_FORMAT = "%s/%s:%s";

    // Azkaban Constants
    public static final String AZKABAN_JOB_ID = "azkaban.job.id";
    public static final String AZKABAN_MAX_RETRIES = "retries";
    public static final String AZKABAN_CURRENT_RETRY_ATTEMPT = "azkaban.job.attempt";
    public static final String AZKABAN_IN_NODES = "azkaban.job.innodes";
    public static final String AZKABAN_FAILURE_ACTION = "failureAction";
    public static final String AZKABAN_CONCURRENT_OPTION = "concurrentOption";
    public static final String APPLICATION_CLASS = "JobDriver.applicationClass";
    public static final String APPLICATION_CLASS_ARGS = "JobDriver.applicationClass.args";
    public static final String APPLICATION_CLASS_DYNAMIC_ARGS = "JobDriver.applicationClass.dynamic.args";
    public static final String AZKABAN_JAR_VERSION = "azkaban.jar.version";
    public static final String ENV = "dsp.env";


    // delimiter
    public static final String dot = ".";
    public static final String equal = "=";
    public static final String colon = ":";
    public static final String comma = ",";
    public static final String slash = "/";
    public static final String hidden = "/.";
    public static final String newLine = "\n";
    public static final String underscore = "_";
    public static final String questionMark = "?";

    // Hive/Hadoop Constants
    public static final String HADOOP_CLUSTER = "hadoop_cluster";
    public static final String PRODUCTION_HIVE_QUEUE = "pds";
    public static final String HIVE_TABLE_VIEW_FORMAT = "%s.%s";
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    public static final String HIVE_QUERY_DATABASE = "hive_query_database";
    public static final String HIVE_VIEW_TITLE_FORMAT = "%s_%s_%d";
    public static final String PARTITION_COLUMNS = "partitionColumn";
    public static final String HIVE_QUERY_TABLE = "dummy_hive_query_table";
    public static final String hadoopcluster_PATH_PREFIX = "projects/planning/";
    public static final String HADOOP_USER_PROPERTY = "hadoop.job.ugi";
    public static final String MAPRED_QUEUE_NAME_PROPERTY = "mapred.job.queue.name";
    public static final String MAPREDUCE_QUEUENAME_PROPERTY = "mapreduce.job.queuename";
    public static final String HDFS_CLUSTER_PREFIX = "hdfs://";
    public static final String ROW_FORMAT_TEMPLATE = "SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
            "WITH SERDEPROPERTIES ('field.delim'='%s','serialization.format'=',')";
    public static final String STORED_AS_TEMPLATE = "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' " +
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'";
    public static final String DELETE_TABLE_TEMPLATE = "DROP TABLE IF EXISTS `%s.%s`";

    // ImageDetails Constant
    public static final String IMAGE_DETAILS_SCRIPT_NAME = "get_image_details.sh";
    public static final String IMAGE_DETAILS_MESOS_APPLICATION = "ImageDetailsMesosApplication";
    public static final String IMAGE_DETAILS_SCRIPT_PATH = "/usr/share/ipp-dsp-workflow-executor/%s/image_details/%s";

    //Blob
    public static final String BLOB_BASE_PATH_PROD = "/projects/planning/dsp_prod/blob";
    public static final String BLOB_BASE_PATH_STAGE = "/projects/planning/dsp_stage/blob";

    public static final String HADOOP_QUERY_DATABASE = "database";

    // Health Check Constant
    public static final String HEALTH_CHECK_ERROR = "Health Check failed for %s";

    //Flow Node Name Constants
    public static final String SG_NODE = "SGNode";
    public static final String OTS_NODE = "OTSNode";
    public static final String PREV_NODE = "prevNode";
    public static final String WORKFLOW_NODE = "WorkflowNode";
    public static final String TERMINAL_NODE = "TerminalNode";
    public static final String NOTIFIER_NODE = "dsp-Notifier";
    public static final String OUTPUT_INGESTION_NODE = "OutputIngestionNode";
    public static final String IMAGE_DETAILS_NODE = "image-details";
    public static final String HEALTH_CHECK_NODE = "HealthCheckNode";

    //EventAudit
    public static final String LOG_PATH_PREFIX = "%s%s:%s%s/";
    public static final String LOG_RESOURCE_PREFIX = "/v1/executions";

    //Size Constants
    private static final int KB = 1024;
    private static final int MB = 1024 * KB;

    // ceph outputIngestion Constants
    public static final int SLEEP = 10;
    public static final int BUFFER_SIZE = 20 * MB;
    public static final int TRANSFER_RETRIES = 3;
    public static final int MULTIPART_SIZE = 20 * MB;
    public static final String DEFAULT_Ceph_FILE_NAME = "output.csv";

    // AuthN Credentials
    public static final String AUTHN_URL = "https://authn.ch.flipkart.com";
    public static final String AUTHN_CLIENT_ID_FILE_NAME = "/etc/authn/%s/authn_client_id";
    public static final String AUTHN_CLIENT_SECRET_KEY_FILE_NAME = "/etc/authn/%s/authn_client_secret_key";

    //Purge Policy Datastores
    public static final String hive = "HIVE";
    public static final String hdfs = "HDFS";

    //Purge Policy Levels
    public static final String TABLE = "TABLE";
    public static final String PARTITION = "PARTITION";

    //Purge Policy Config
    public static final Map<String, String> PURGE_POLICY_TIME_CONFIG = Stream.of(new String[][]{
            {"dsp_sg", "30 DAY"},
            {"dsp_sg_stage", "30 DAY"},
            {"dsp_sg_stage_beta", "30 DAY"},
            {"dsp_output", "30 DAY"},
    }).collect(Collectors.toMap(data -> (String) data[0], data -> data[1]));

    public static final Integer PURGE_POLICY_TIME_OFFSET = 1;  // 1 DAY
    public static final String PURGE_POLICY_PARTITION_COLUMN = "refresh_id";


    // generic
    public static final String ftp = "ftp";
    public static final String tmp = "tmp";
    public static final String sudo = "sudo";
    public static final String http = "http://";
    public static final String execid = "execid";
    public static final String https = "https://";
    public static final String details = "details";
    public static final String executor = "executor";
    public static final String ZIP_EXTENSION = ".zip";
    public static final String SG_JOB_ID = "sg.job.id";
    public static final String HEADERS_KEY = "headers";
    public static final String LOCALHOST = "localhost";
    public static final String REQUEST_ID = "requestId";
    public static final String CSV_EXTENSION = ".csv";
    public static final String REFRESH_ID = "refresh_id";
    public static final String STEP_NAME = "step_name";
    public static final String DATAFRAME_KEY = "dataFrame";
    public static final String DAG_ENTITIES = "dagEntities";
    public static final String DUMMY_CSV_TABLE = "dummy_csv";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String PARTITION_KEYS = "partition_keys";
    public static final String AZKABAN_HOST_IP = "azkaban.host.ip";
    public static final String APPLICATION_JSON = "application/json";
    public static final String PARTITION_VALUES = "partition_values";
    public static final String CONFIG_SVC_BUCKETS_KEY = "dsp.svc.buckets";

    public static final String DEFAULT_FILE_SYSTEM = "fs.defaultFS";
    //for creating configuration, DSP's default queue
    public static final String PRODUCTION_DEFAULT_USER = "prod.default.user";
    // all request from sandbox will run on this mesos queue
    public static final String EXPERIMENTATION_MESOS_QUEUE = "experimentation";
    public static final String NOTIFICATION_EMAIL_HOST = "notifier.email.host";
    public static final String LOG_EXECUTION_PATH = "/v1/executions/%s/logs?workflow-id=%s";
    public static final JavaType listType = JsonUtils.DEFAULT.mapper.getTypeFactory().constructCollectionType(List.class, String.class);

}

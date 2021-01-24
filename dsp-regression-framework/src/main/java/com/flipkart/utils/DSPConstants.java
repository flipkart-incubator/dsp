package com.flipkart.utils;

public class DSPConstants {
    // PlaceHolders
    public static final String IS_PROD = "IS_PROD";
    public static final String Ceph_ALIAS = "Ceph_ALIAS";
    public static final String DSP_JOB_ID = "DSP_JOB_ID";
    public static final String DSP_API_URL = "DSP_API_URL";
    public static final String WORKFLOW_NAME = "WORKFLOW_NAME";
    public static final String YAML_LOCAL_PATH = "YAML_LOCAL_PATH";
    public static final String SANDBOX_JAR_PATH = "SANDBOX_JAR_PATH";
    public static final String LOCAL_SCRIPT_FOLDER = "LOCAL_SCRIPT_FOLDER";
    public static final String SMALL_CSV_FILE_LOCAL_PATH = "SMALL_CSV_FILE_LOCAL_PATH";
    public static final String LARGE_CSV_FILE_LOCAL_PATH = "LARGE_CSV_FILE_LOCAL_PATH";
    public static final String DSP_PLATFORM_RUN_PAYLOAD = "WORKFLOW_GROUP_RUN_PAYLOAD";
    public static final String DSP_PLATFORM_PROMOTE_PAYLOAD = "DSP_PLATFORM_PROMOTE_PAYLOAD";

    // Sandbox Constants
    public static final String BUCKET_POSTFIX = "bucket_postfix";
    public static final String SANDBOX_ENVIRONMENT = "--dev:env";
    public static final String SANDBOX_RUN_COMMAD = "--flows:run";
    public static final String SANDBOX_PROMOTE_COMMAD = "--flows:promote";
    public static final String SANDBOX_CREATE_CREDENTIALS_COMMAND = "--flows:create_credentials";
    public static final String SANDBOX_CLI_JAR_FORMAT = "dsp-sandbox-%s.jar";
    public static final String SANDBOX_CLI_JAR_ART_FORMAT = "%s/%s/dsp-sandbox-%s.jar";
    public static final String SANDBOX_ART_URL_PREFIX = "http://0.0.0.0/artifactory/v1.0/artifacts/libs-release-local/com/flipkart/dsp/dsp-sandbox";

    // Platform API Calls
    public static final String RUN_STATUS_URL_FORMAT = "http://%s:9090/v1/executions/%s";
    public static final String DSP_SERVICE_HEALTH_CHECK_URL_FORMAT = "http://%s:9091/healthcheck";
    public static final String RUN_WORKFLOW_URL_FORMAT = "http://%s:9090/v1/workflow/execute";
    public static final String RUN_WORKFLOW_GROUP_URL_FORMAT = "http://%s:9090/v2/workflowGroups/%s/run?draft=%s";
    public static final String PROMOTE_WORKLFOW_GROUP_VIA_PLATFORM_URL_FORMAT = "http://%s:9090/v1/executions/%s/promote";

    // Config Bucket / Repo service constant
    public static final String DSP_CONFIG_BUCKET_FORMAT = "dsp-%s";
    public static final String CONFIG_BUCKET_LATEST_VERSION_URL_FORMAT = "http://0.0.0.0/v1/buckets/%s/meta";
    public static final String CONFIG_BUCKET_UPDATE_VERSION_URL_FORMAT = "http://0.0.0.0/v1/buckets/%s/keys?message=%s";
    public static final String REPO_LATEST_VERSION_URL_FORMAT = "http://0.0.0.0:8080/repo/%s/HEAD?appkey=clientkey";
    public static final String REPO_PACKAGE_VERSION_URL_FORMAT = "http://0.0.0.0:8080/pkgs/repo/%s/%s?appkey=clientkey";

    // Platform Constants
    public static final String MESOS_VERSION = "mesos_version";
    public static final String MESOS_PACKAGE = "mesos_package";
    public static final String SERVICE_VERSION = "service_version";
    public static final String AZKABAN_VERSION = "azkaban_version";
    public static final String AZKABAN_PACKAGE = "azkaban_package";
    public static final String EXECUTION_ENVIRONMENT = "execution_environment";
    public static final String SERVICE_REPO_PACKAGE_NAME = "%s-ipp-dsp-service-repo";
    public static final String AZKABAN_REPO_PACKAGE_NAME = "%s-ipp-dsp-azkaban-repo";
    public static final String MESOS_REPO_PACKAGE_NAME = "%s-ipp-dsp-workflow-executor";
    public static final String MESOS_ROLE_QUOTA_UPDATE_URL_FORMAT = "http://%s:5050/quota";

    // misc constants
    public static final String ENV_FILE_NAME = ".env";
    public static final String USER_HOME = "user.home";
    public static final String LOCAL_BASE_PATH= "%s/dsp-regression";
    public static final String REGRESSION_HOST_NAME = "regression-host";
    public static final String LOCAL_FOLDER_LOCATION = LOCAL_BASE_PATH + "/%s";
    public static final String TEST_BED_RESOURCE_PATH = "/fixtures/dsp/testbed";
    public static final String ENV_FILE_RELATIVE_PATH = TEST_BED_RESOURCE_PATH + "/template.env";
    public static final String INIT_SCRIPT_RELATIVE_PATH = TEST_BED_RESOURCE_PATH + "/initScript.sh";
    public static final String DOCKER_COMPOSE_FILE_RELATIVE_PATH = TEST_BED_RESOURCE_PATH + "/docker-compose.yml";

}

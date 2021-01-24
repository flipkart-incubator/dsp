package com.flipkart.utils;

import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.config.GithubConfig;
import com.flipkart.dsp.config.HiveConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.models.workflow.WorkflowPromoteRequest;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dto.DSPTestEnvVariables;
import com.flipkart.dto.TestEnvVariables;
import com.flipkart.enums.DSPTestScenarioEnum;
import com.flipkart.exception.ConfigBucketException;
import com.flipkart.exception.FileOperationException;
import com.flipkart.exception.TestBedException;
import com.flipkart.exception.TestScenarioExecutionException;
import com.flipkart.manager.ConfigBucketManager;
import com.flipkart.manager.JarManager;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.dot;
import static com.flipkart.utils.DSPConstants.*;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DSPHelper {
    private final HiveConfig hiveConfig;
    private final HiveClient hiveClient;
    private final JarManager jarManager;
    private final GithubConfig githubConfig;
    private final MetaStoreClient metaStoreClient;
    private final ConfigBucketManager configBucketManager;
    private final FileOperationsHelper fileOperationsHelper;

    public void updatePlatformBucket(Object payload, String bucket) throws ConfigBucketException {
        Long version = configBucketManager.getConfigBucketLatestVersion(bucket);
        configBucketManager.updateConfigBucket(payload, bucket, "updationOfProperty", version);
    }

    public Map<String, String> getDockerComposePlaceHolders(String bucketPostfix, DSPTestEnvVariables envVariables) {
        Map<String, String> dockerComposePlaceHolders = new HashMap<>();
        dockerComposePlaceHolders.put(BUCKET_POSTFIX, bucketPostfix);
        dockerComposePlaceHolders.put(EXECUTION_ENVIRONMENT, envVariables.getExecutionEnvironment());
        dockerComposePlaceHolders.put(SERVICE_VERSION, envVariables.getServiceDebianVersion().toString());
        dockerComposePlaceHolders.put(AZKABAN_VERSION, envVariables.getAzkabanDebianVersion().toString());
        dockerComposePlaceHolders.put(AZKABAN_PACKAGE, envVariables.getAzkabanPackageVersion());
        dockerComposePlaceHolders.put(MESOS_VERSION, envVariables.getMesosAgentDebianVersion().toString());
        dockerComposePlaceHolders.put(MESOS_PACKAGE, envVariables.getMesosAgentPackageVersion());
        return dockerComposePlaceHolders;
    }

    public String getTempFolder() {
        String runFolderName = String.valueOf(new Date().getTime());
        String userHomeDir = System.getProperty(USER_HOME);
        return String.format(LOCAL_FOLDER_LOCATION, userHomeDir, runFolderName);
    }

    public String preProcessingForSandbox(TestEnvVariables testEnvVariables) throws IOException, FileOperationException, TestBedException {
        String sandboxCliVersion = ((DSPTestEnvVariables)testEnvVariables).getSandboxCliVersion();
        if (sandboxCliVersion != null && !sandboxCliVersion.isEmpty()) {
            String jarUrl = String.format(SANDBOX_CLI_JAR_ART_FORMAT, SANDBOX_ART_URL_PREFIX, sandboxCliVersion, sandboxCliVersion);
            String localJarName = String.format(SANDBOX_CLI_JAR_FORMAT, sandboxCliVersion);
            Path sandboxLocalPath = Paths.get("/tmp", localJarName);
            jarManager.downloadJarFromUriToLocalPath(sandboxLocalPath, jarUrl);
            generateOAuth();
            return sandboxLocalPath.toString();
        }
        throw new TestBedException("Cli version can't be null in Request Payload");
    }

    private void generateOAuth() throws FileOperationException {
        String userHomeDir = System.getProperty("user.home");
        String tmpFolderPath = Paths.get( userHomeDir, ".dsp-sandbox").toString();
        fileOperationsHelper.createDirectoryIfNotExist(tmpFolderPath);
        Path filePath = Paths.get(tmpFolderPath, "OAuth.txt");
        fileOperationsHelper.writeToFile(filePath, githubConfig.getOAuthToken());
    }

    public void addInputsToTestCase(DSPTestScenarioEnum testcase, Map<String, Object> testInputs) throws TestScenarioExecutionException {
        switch (testcase) {
            case RUN_2_0_YAML_SANDBOX_CASE_1:
            case RUN_2_0_YAML_SANDBOX_CASE_2:
            case RUN_2_1_YAML_SANDBOX_CASE_1:
            case RUN_2_1_YAML_SANDBOX_CASE_2:
                preProcessYaml(testcase, testInputs);
                break;

            case RUN_PROD_WORKFLOW_PLATFORM:
            case RUN_DRAFT_WORKFLOW_PLATFORM:
            case RUN_PROD_WORKFLOW_GROUP_PLATFORM:
            case RUN_DRAFT_WORKFLOW_GROUP_PLATFORM:
                preProcessPlatformRunPayload(testcase, testInputs);
                break;
            case PROMOTE_2_0_FLOW_ON_PLATFORM:
            case PROMOTE_2_1_FLOW_ON_PLATFORM:
                preProcessPlatformPromotePayload(testcase, testInputs);
                break;
            default:
                break;
        }
    }

    private void preProcessYaml(DSPTestScenarioEnum dspTestScenarioEnum, Map<String, Object> testInputs) throws TestScenarioExecutionException {
        String yamlPath = DSPTestScenarioEnum.getInputFileName(dspTestScenarioEnum);
        String userHomeDir = System.getProperty(USER_HOME);
        String tempFolderPath = String.format(LOCAL_FOLDER_LOCATION, userHomeDir, "test-cases");
        String yamlFileName = yamlPath.substring(yamlPath.lastIndexOf('/') + 1);
        Path localFilePath = Paths.get(tempFolderPath, yamlFileName);

        try {
            fileOperationsHelper.createDirectoryIfNotExist(tempFolderPath);
            copyRequiredFiles(tempFolderPath, dspTestScenarioEnum);
            String yamlFileContent = fileOperationsHelper.getFileContent(yamlPath);
            yamlFileContent = updatePlaceHoldersInYaml(yamlFileContent, tempFolderPath, testInputs);
            fileOperationsHelper.writeToFile(localFilePath, yamlFileContent);
            testInputs.put(YAML_LOCAL_PATH, localFilePath.toString());
        } catch (FileOperationException e) {
            throw new TestScenarioExecutionException("Error while preprocessing Yaml For Test Scenario: " + dspTestScenarioEnum.name());
        }
    }

    private void copyRequiredFiles(String tempFolderPath, DSPTestScenarioEnum dspTestScenarioEnum) throws FileOperationException {
        if (dspTestScenarioEnum.equals(DSPTestScenarioEnum.RUN_2_0_YAML_SANDBOX_CASE_2)
                || dspTestScenarioEnum.equals(DSPTestScenarioEnum.RUN_2_1_YAML_SANDBOX_CASE_2)) {
            fileOperationsHelper.getFileFromResource("/fixtures/dsp/test-cases/small_csv.csv", tempFolderPath);
            fileOperationsHelper.getFileFromResource("/fixtures/dsp/test-cases/large_csv.csv", tempFolderPath);
        }

        if (dspTestScenarioEnum.equals(DSPTestScenarioEnum.RUN_2_0_YAML_SANDBOX_CASE_2))
            fileOperationsHelper.getFileFromResource("/fixtures/dsp/test-cases/version_2_0_case_2.py", tempFolderPath);
    }

    private String updatePlaceHoldersInYaml(String yamlFileContent, String tempFolderPath, Map<String, Object> testInputs) {
        yamlFileContent = yamlFileContent.replace(Ceph_ALIAS, testInputs.get(Ceph_ALIAS).toString());
        yamlFileContent = yamlFileContent.replace(LOCAL_SCRIPT_FOLDER, String.format(LOCAL_BASE_PATH, System.getProperty(USER_HOME)));
        yamlFileContent = yamlFileContent.replace(SMALL_CSV_FILE_LOCAL_PATH, tempFolderPath + "/small_csv.csv");
        yamlFileContent = yamlFileContent.replace(LARGE_CSV_FILE_LOCAL_PATH, tempFolderPath + "/large_csv.csv");
        return yamlFileContent;
    }

    private void preProcessPlatformRunPayload(DSPTestScenarioEnum dspTestScenarioEnum, Map<String, Object> testInputs) throws TestScenarioExecutionException {
        try {
            String content = fileOperationsHelper.getFileContent(DSPTestScenarioEnum.getInputFileName(dspTestScenarioEnum));
            content = content.replace(WORKFLOW_NAME, "REGRESSION_SANDBOX_TEST_WF");
            content = replaceIsProd(content, dspTestScenarioEnum);
            testInputs.put(DSP_API_URL, DSPTestScenarioEnum.getPlatformCallUrl(dspTestScenarioEnum));
            testInputs.put(DSP_PLATFORM_RUN_PAYLOAD, DSPTestScenarioEnum.getRunPayload(content, dspTestScenarioEnum));
        } catch (FileOperationException e) {
            throw new TestScenarioExecutionException("Error while preprocessing For Test Scenario: " + dspTestScenarioEnum.name());
        }
    }

    private String replaceIsProd(String content, DSPTestScenarioEnum dspTestScenarioEnum) {
        if (dspTestScenarioEnum.equals(DSPTestScenarioEnum.RUN_PROD_WORKFLOW_PLATFORM))
            content = content.replace(IS_PROD, "true");
        else if (dspTestScenarioEnum.equals(DSPTestScenarioEnum.RUN_DRAFT_WORKFLOW_PLATFORM))
            content = content.replace(IS_PROD, "false");
        return content;
    }

    private void preProcessPlatformPromotePayload(DSPTestScenarioEnum dspTestScenarioEnum, Map<String, Object> testInputs) throws TestScenarioExecutionException {
        try {
            String content = fileOperationsHelper.getFileContent(DSPTestScenarioEnum.getInputFileName(dspTestScenarioEnum));
            testInputs.put(DSP_PLATFORM_PROMOTE_PAYLOAD, JsonUtils.DEFAULT.fromJson(content, WorkflowPromoteRequest.class));
        } catch (FileOperationException e) {
            throw new TestScenarioExecutionException("Error while preprocessing For Test Scenario: " + dspTestScenarioEnum.name());
        }
    }

    public void deleteHiveSGTableIfExist() throws HiveClientException, TException {
        List<String> existingSgTables = getExistingSgTables();
        for (String sgTable : existingSgTables) {
            deleteTable(hiveConfig.getSgDatabase() + dot + sgTable);
        }
    }

    private List<String> getExistingSgTables() throws TException {
        List<String> allTablesToDelete = new ArrayList<>();
        List<String> testDataFrames = getDataFrames();
        for (String dataFrame: testDataFrames) {
            allTablesToDelete.addAll(metaStoreClient.getTablesByPattern(hiveConfig.getSgDatabase(), dataFrame + "*"));
        }
        log.info("ALL Tables To delete: " + String.join(",", allTablesToDelete));
        return allTablesToDelete;
    }


    private List<String> getDataFrames() {
        List<String> dataFrames = new ArrayList<>();
        dataFrames.add("regression_hive_table_input_test_dataframe");
        dataFrames.add("regression_ddp_table_input_test_dataframe");
        dataFrames.add("regression_fdp_input_test_dataframe");
        dataFrames.add("regression_hive_query_input_test_dataframe");
        dataFrames.add("regression_fdp_dataset_input_test_dataframe");
        dataFrames.add("regression_csv_in_memory_input_test_dataframe");
        dataFrames.add("regression_csv_map_reduce_input_test_dataframe");
        dataFrames.add("regression_csv_map_reduce_input_test_dataframe");
        dataFrames.add("regression_hive_table_output_test_dataframe_step_1_case_1");
        dataFrames.add("regression_hive_table_output_test_dataframe_step_1_case_2");
        return dataFrames;
    }

    private void deleteTable(String tempTableName) throws HiveClientException {
        String dropCommand = String.format("DROP TABLE IF EXISTS %s", tempTableName);
        log.info("Deleting Hive Table: " + tempTableName);
        hiveClient.executeQuery(dropCommand);
    }

}

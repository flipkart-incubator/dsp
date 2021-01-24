package com.flipkart.team.dsp;

import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dto.ConfigUpdatePayload;
import com.flipkart.dto.DSPTestEnvVariables;
import com.flipkart.dto.TestEnvVariables;
import com.flipkart.dto.TestExecutionDetails;
import com.flipkart.enums.DSPTestScenarioEnum;
import com.flipkart.enums.TestExecutionStatus;
import com.flipkart.exception.*;
import com.flipkart.manager.*;
import com.flipkart.team.Team;
import com.flipkart.testScenario.TestScenario;
import com.flipkart.utils.DSPConstants;
import com.flipkart.utils.DSPHelper;
import com.flipkart.utils.FileOperationsHelper;
import com.flipkart.utils.ScheduleDSPNotifier;
import com.google.inject.Inject;
import com.google.inject.Provider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.utils.DSPConstants.*;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DSPTeam extends Team {
    private final DSPHelper dspHelper;
    private final MiscConfig miscConfig;
    private final ReportManager reportManager;
    private final TestBedManager testBedManager;
    private final DSPHealthCheck dspHealthCheck;
    private final MesosRoleManager mesosRoleManager;
    private final RepoServiceManager repoServiceManager;
    private final ScheduleDSPNotifier scheduleDSPNotifier;
    private final FileOperationsHelper fileOperationsHelper;
    private final Map<DSPTestScenarioEnum, Provider<TestScenario>> commandMap;

    private String bucketPostfix = "regression";

    // Start a notifier node in a separate thread
    @Override
    public void performRegressionTest(TestEnvVariables envVariables) throws Exception {
        try {
            DSPTestEnvVariables dspTestEnvVariables = (DSPTestEnvVariables) envVariables;
            getLatestDebianVersion(envVariables);
            String tempFolderPath = dspHelper.getTempFolder();
            fileOperationsHelper.createDirectoryIfNotExist(tempFolderPath);
            String composeFilePath = fileOperationsHelper.getFileFromResource(DOCKER_COMPOSE_FILE_RELATIVE_PATH, tempFolderPath);
            String envTemplateFile = fileOperationsHelper.getFileFromResource(ENV_FILE_RELATIVE_PATH, tempFolderPath);

            composeEnv(composeFilePath, envTemplateFile, tempFolderPath, envVariables);
            verifyEnvSetup(composeFilePath);

            scheduleDSPNotifier.scheduleNotifier(bucketPostfix, dspTestEnvVariables.getAzkabanPackageVersion());

            List<String> testCasesToBeExecuted = identifyTestCases(dspTestEnvVariables);
            List<TestExecutionDetails> testExecutionDetailsList = executeTestCases(envVariables, testCasesToBeExecuted);
            String recipientEmailId = (envVariables.getEmailId() == null || envVariables.getEmailId().isEmpty()) ? miscConfig.getDefaultNotificationEmailId()
                    : envVariables.getEmailId();
            reportManager.generateReports(testExecutionDetailsList, recipientEmailId);
            if (envVariables.destroyEnv) {
                String composeFileFolderPath = composeFilePath.substring(0, composeFilePath.lastIndexOf("/"));
                boolean destroyEnv = testBedManager.distoryEnv(composeFileFolderPath);
                if (destroyEnv) {
                    log.info("Environment destroyed Successfully");
                }
            }
            boolean testFailed = hasAnyTestFailed(testExecutionDetailsList);
            if (testFailed) {
                throw new TestCaseFailureException("Jar is failed as one of the test case failed to execute");
            }
        } catch (TestCaseFailureException testCaseException) {
            throw new Exception(testCaseException.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            reportManager.generateInternalFailureMail(envVariables.getEmailId(), e);
            throw new Exception(e.getMessage(), e);
        }
    }

    @Override
    protected void composeEnv(String composeFileAbsolutePath, String envTemplateFile, String tempFolderPath,
                              TestEnvVariables envVariables) throws TestBedException, FileOperationException {
        //   * .env file needed for compose will be created in same folder as that of docker-compose.yml file
        Map<String, String> dockerComposePlaceHoldersMap = dspHelper.getDockerComposePlaceHolders(bucketPostfix, (DSPTestEnvVariables) envVariables);
        String dockerComposeFolderPath = composeFileAbsolutePath.substring(0, composeFileAbsolutePath.lastIndexOf("/"));
        String initFileAbsolutePath = fileOperationsHelper.getFileFromResource(INIT_SCRIPT_RELATIVE_PATH, tempFolderPath);
        testBedManager.executeScript(initFileAbsolutePath);
        testBedManager.createEnvFileForDockerCompose(envTemplateFile, dockerComposeFolderPath, dockerComposePlaceHoldersMap);
        testBedManager.pruneExistingDockerNetwork();
        testBedManager.initiateDockerCompose(dockerComposeFolderPath);
        testBedManager.addExtraResourcesForSGExecution();
        mesosRoleManager.updateMesosRoleQuota();
        log.info("Environment setup completed");
    }

    @Override
    protected void verifyEnvSetup(String composeFilePath) throws TestBedException, HealthCheckFailedException {
        log.info("Waiting for service to come up");
        try {
            Thread.sleep(40000);
            log.info("Sleeping over");
        } catch (InterruptedException e) {
            // Nothing to worry we can ignore this
            log.error(e.getMessage());
        }
        String serviceUrl = String.format(DSPConstants.DSP_SERVICE_HEALTH_CHECK_URL_FORMAT, DSPConstants.REGRESSION_HOST_NAME);
        dspHealthCheck.service(serviceUrl);
    }

    @Override
    protected List<String> identifyTestCases(TestEnvVariables testEnvVariables) throws TestScenarioNotFoundException {
        DSPTestScenarioEnum[] listOfTestCases = DSPTestScenarioEnum.values();
        List<String> possibleTestCases = Arrays.stream(listOfTestCases).map(Enum::toString).collect(Collectors.toList());

        List<String> testCasesToBeExecuted;
        if (Boolean.TRUE.equals(testEnvVariables.getExecuteAllTestScenarios())) {
            testCasesToBeExecuted = possibleTestCases;
        } else {
            testCasesToBeExecuted = new ArrayList<>();
            for (String specificTestCase : testEnvVariables.getTestScenarioList()) {
                if (possibleTestCases.contains(specificTestCase)) {
                    testCasesToBeExecuted.add(specificTestCase);
                } else {
                    throw new TestScenarioNotFoundException(specificTestCase + " not a valid Scenario name.Possible Scenario are " + possibleTestCases);
                }
            }
            if (testCasesToBeExecuted.isEmpty()) {
                throw new TestScenarioNotFoundException("No Test Scenario Selected.Possible Scenario are " + possibleTestCases);
            }
        }
        return testCasesToBeExecuted;
    }

    @Override
    protected List<TestExecutionDetails> executeTestCases(TestEnvVariables testEnvVariables, List<String> testCaseList)
            throws IOException, FileOperationException, TestBedException, HiveClientException, TestScenarioExecutionException, TException {
        String sandboxLocalPath = dspHelper.preProcessingForSandbox(testEnvVariables);
        dspHelper.deleteHiveSGTableIfExist();

        List<TestExecutionDetails> testExecutionDetailsList = new ArrayList<>();
        Map<String, Object> testInputs = new HashMap<>();
        testInputs.put(BUCKET_POSTFIX, bucketPostfix);
        testInputs.put(SANDBOX_JAR_PATH, sandboxLocalPath);

        for (String testCase : testCaseList) {
            dspHelper.addInputsToTestCase(DSPTestScenarioEnum.valueOf(testCase), testInputs);
            TestExecutionDetails testExecutionDetails = commandMap.get(DSPTestScenarioEnum.valueOf(testCase)).get().run(testInputs, testCase);
            if ((DSPTestScenarioEnum.valueOf(testCase) == DSPTestScenarioEnum.CREATE_EXTERNAL_CREDENTIALS_SANDBOX))
                testInputs.put(Ceph_ALIAS, testExecutionDetails.getExtraDetails());

            if ((DSPTestScenarioEnum.valueOf(testCase) == DSPTestScenarioEnum.RUN_2_0_YAML_SANDBOX_CASE_1)
                    || (DSPTestScenarioEnum.valueOf(testCase) == DSPTestScenarioEnum.RUN_2_1_YAML_SANDBOX_CASE_1))
                testInputs.put(DSP_JOB_ID, testExecutionDetails.getExtraDetails());

            testExecutionDetailsList.add(testExecutionDetails);
        }
        return testExecutionDetailsList;
    }

    @Override
    protected void getLatestDebianVersion(TestEnvVariables testEnvVariables) throws RepoServiceException, ConfigBucketException {
        DSPTestEnvVariables dspTestEnvVariables = (DSPTestEnvVariables) testEnvVariables;
        String dspBucket;
        if (((DSPTestEnvVariables) testEnvVariables).getBucketPostfix() == null || ((DSPTestEnvVariables) testEnvVariables).getBucketPostfix().isEmpty()) {
            dspBucket = String.format(DSPConstants.DSP_CONFIG_BUCKET_FORMAT, bucketPostfix);
        } else {
            bucketPostfix = ((DSPTestEnvVariables) testEnvVariables).getBucketPostfix();
            dspBucket = String.format(DSPConstants.DSP_CONFIG_BUCKET_FORMAT, bucketPostfix);
        }

        if (dspTestEnvVariables.getAzkabanDebianVersion() == null || dspTestEnvVariables.getAzkabanDebianVersion() == 0) {
            Long debianVersion = repoServiceManager.getLatestRepoVersion(AZKABAN_REPO_PACKAGE_NAME, dspTestEnvVariables.getExecutionEnvironment());
            String azkabanPackageVersion = repoServiceManager.getPkgVersionForSpecificRepo(AZKABAN_REPO_PACKAGE_NAME,
                    dspTestEnvVariables.getExecutionEnvironment(), debianVersion);

            dspTestEnvVariables.setAzkabanDebianVersion(debianVersion);
            dspTestEnvVariables.setAzkabanPackageVersion(azkabanPackageVersion);
            ConfigUpdatePayload configUpdatePayload = ConfigUpdatePayload.builder().azkabanJarVersion(azkabanPackageVersion).build();
            dspHelper.updatePlatformBucket(configUpdatePayload, dspBucket);
        }

        if (dspTestEnvVariables.getMesosAgentDebianVersion() == null || dspTestEnvVariables.getMesosAgentDebianVersion() == 0) {
            Long debianVersion = repoServiceManager.getLatestRepoVersion(MESOS_REPO_PACKAGE_NAME, dspTestEnvVariables.getExecutionEnvironment());
            String packageVersion = repoServiceManager.getPkgVersionForSpecificRepo(MESOS_REPO_PACKAGE_NAME,
                    dspTestEnvVariables.getExecutionEnvironment(), debianVersion);
            log.info("Mesos Package Version: " + packageVersion);

            dspTestEnvVariables.setMesosAgentDebianVersion(debianVersion);
            dspTestEnvVariables.setMesosAgentPackageVersion(packageVersion);
            ConfigUpdatePayload configUpdatePayload = ConfigUpdatePayload.builder().executorJarVersion(packageVersion).build();
            dspHelper.updatePlatformBucket(configUpdatePayload, dspBucket);
        }

        if (dspTestEnvVariables.getServiceDebianVersion() == null || dspTestEnvVariables.getServiceDebianVersion() == 0) {
            dspTestEnvVariables.setServiceDebianVersion(repoServiceManager.getLatestRepoVersion(SERVICE_REPO_PACKAGE_NAME, dspTestEnvVariables.getExecutionEnvironment()));
        }
    }

    private boolean hasAnyTestFailed(List<TestExecutionDetails> testExecutionDetailsList) {
        for (TestExecutionDetails testExecutionDetails : testExecutionDetailsList) {
            if (testExecutionDetails.getTestExecutionStatus() == TestExecutionStatus.FAILED) {
                return true;
            }
        }
        return false;
    }
}

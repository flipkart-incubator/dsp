package com.flipkart.team;

import com.flipkart.dto.TestEnvVariables;
import com.flipkart.dto.TestExecutionDetails;
import com.flipkart.exception.FileOperationException;
import com.flipkart.exception.TestBedException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ha.HealthCheckFailedException;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;

@Slf4j
public abstract class Team {
    protected abstract void composeEnv(String composeFilePath, String envTemplateFile,String tempFolderPath,
                                       TestEnvVariables envVariables) throws TestBedException, UnknownHostException, URISyntaxException, FileOperationException;
    protected abstract void verifyEnvSetup(String composeFilePath) throws TestBedException, HealthCheckFailedException;
    protected abstract List<TestExecutionDetails> executeTestCases(TestEnvVariables envVariables, List<String> testCaseList) throws Exception;
    protected abstract List<String> identifyTestCases(TestEnvVariables testEnvVariables) throws Exception;

    /**
     * Override this function in case you
     * @param testEnvVariables
     * @throws Exception
     */
    protected abstract void getLatestDebianVersion(TestEnvVariables testEnvVariables) throws Exception;
    public abstract void performRegressionTest(TestEnvVariables envVariables) throws Exception;
}

package com.flipkart.testScenario.dsp;

import com.flipkart.dto.TestRunDetails;
import com.flipkart.enums.TestExecutionStatus;
import com.flipkart.manager.ComparisonManager;
import com.flipkart.manager.JarManager;
import com.flipkart.testScenario.TestScenario;
import com.flipkart.utils.CompareType;
import com.flipkart.utils.DSPConstants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Map;

import static com.flipkart.utils.DSPConstants.BUCKET_POSTFIX;
import static com.flipkart.utils.DSPConstants.SANDBOX_JAR_PATH;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class CreateCredentialsViaSandbox extends TestScenario {
    private final JarManager jarManager;
    private final ComparisonManager comparisonManager;


    @Override
    protected void runTestCase(Map<String, Object> input, TestRunDetails testRunDetails) throws Exception {
        log.info("Test Description: " + testRunDetails.getTestDescription());

        String[] runCommand = new String[14];
        runCommand[0] = "java";
        runCommand[1] = "-jar";
        runCommand[2] = input.get(SANDBOX_JAR_PATH).toString();
        runCommand[3] = DSPConstants.SANDBOX_ENVIRONMENT;
        runCommand[4] = input.get(BUCKET_POSTFIX).toString();
        runCommand[5] = DSPConstants.SANDBOX_CREATE_CREDENTIALS_COMMAND;
        runCommand[6] = "-entity";
        runCommand[7] = "Ceph";
        runCommand[8] = "-host";
        runCommand[9] = "0.0.0.0";
        runCommand[10] = "-access_key";
        runCommand[11] = "\"AAAAAAAAAAAAAAAAAAAA\"";
        runCommand[12] = "-secret_key";
        runCommand[13] = "\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/ou/\"";

        log.info(" Command Created to execute test Scenario 1 " + Arrays.toString(runCommand));
        Object output = jarManager.executeJar(runCommand, "Issue in Creating External Credentials");

        for (String s : output.toString().split("\\.")) {
            String searchString = "Credentials alias for entity:";
            if (s.contains(searchString)) {
                String cephAlias = s.substring(s.indexOf(searchString) + searchString.length() + 2, s.length() - 1);
                log.info("\nceph_alias: " + cephAlias);
                testRunDetails.setActualResult("COMPLETED");
                testRunDetails.setTestExecutionStatus(TestExecutionStatus.PASSED);
                testRunDetails.setExtraDetails(cephAlias);
                return;
            }
        }
        testRunDetails.setTestExecutionStatus(TestExecutionStatus.FAILED);
        testRunDetails.setActualResult("FAILED");
    }

    @Override
    protected Object loadExpectedResult() {
        return "COMPLETED";
    }

    @Override
    protected boolean assertEqual(Object expectedValue, Object actualValue) {
        return comparisonManager.compare(expectedValue, actualValue, CompareType.TEXT);
    }
}

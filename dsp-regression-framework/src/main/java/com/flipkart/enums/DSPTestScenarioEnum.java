package com.flipkart.enums;

import com.flipkart.dsp.models.WorkflowGroupExecuteRequest;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.exception.TestScenarioExecutionException;
import com.flipkart.utils.DSPConstants;

public enum DSPTestScenarioEnum {
    CREATE_EXTERNAL_CREDENTIALS_SANDBOX() {
        @Override
        public String getTestDescription() {
            return "Test will create a Ceph credentials by Sandbox.";
        }

    },
    RUN_2_0_YAML_SANDBOX_CASE_1() {
        @Override
        public String getTestDescription() {
            return "Test will execute a 2.0 version Yaml via Sandbox CLI.\n"
                    + "Inputs: HIVE,DDP,HIVE_QUERY,STRING,INT,DOUBLE,BOOLEAN,DATE_TIME,DATE,LONG\n"
                    + "Outputs: DEFAULT HDFS, HDFS PATH, HIVE, DDP, Ceph.\n"
                    + "Partitioned: False, Remote Script\n";
        }

    },
    RUN_DRAFT_WORKFLOW_GROUP_PLATFORM() {
        @Override
        public String getTestDescription() {
            return "Test will run a draft workflow_group for RUN_2_0_YAML_SANDBOX_CASE_1.";
        }

    },
    PROMOTE_2_0_FLOW_BY_SANDBOX() {
        @Override
        public String getTestDescription() {
            return "Test will promote a successful workflow via Sandbox CLI for Yaml 2.0.";
        }

    },
    PROMOTE_2_0_FLOW_ON_PLATFORM() {
        @Override
        public String getTestDescription() {
            return "Test will promote a workflow on Platform.";
        }

    },
    RUN_PROD_WORKFLOW_GROUP_PLATFORM() {
        @Override
        public String getTestDescription() {
            return "Test will run a prod workflow_group for RUN_2_0_YAML_SANDBOX_CASE_1.";
        }

    },
    RUN_2_0_YAML_SANDBOX_CASE_2() {
        @Override
        public String getTestDescription() {
            return "Test will execute a 2.0 version Yaml via Sandbox CLI.\n"
                    + "Inputs: SMALL_CSV,LARGE_CSV,DEFAULT_DF. \n"
                    + "Partitioned: False, Local Script\n";
        }
    },
    RUN_2_1_YAML_SANDBOX_CASE_1() {
        @Override
        public String getTestDescription() {
            return "Test will execute a 2.1 version Yaml via Sandbox CLI.\n"
                    + "Inputs: FDP,FDP_DATASET,HIVE,DDP,HIVE_QUERY,STRING,INT,DOUBLE,BOOLEAN,DATE_TIME,DATE,LONG\n"
                    + "Outputs: DEFAULT HDFS, HDFS PATH, HIVE, DDP, Ceph\n"
                    + "Partitioned: True, Multi PipelineStep, Remote Script\n";
        }

    },
    RUN_DRAFT_WORKFLOW_PLATFORM() {
        @Override
        public String getTestDescription() {
            return "Test will run a draft workflow for RUN_2_1_YAML_SANDBOX_CASE_1.";
        }

    },
    PROMOTE_2_1_FLOW_BY_SANDBOX() {
        @Override
        public String getTestDescription() {
            return "Test will promote a successful workflow via Sandbox CLI.";
        }

    },
    PROMOTE_2_1_FLOW_ON_PLATFORM() {
        @Override
        public String getTestDescription() {
            return "Test will promote a workflow on Platform.";
        }

    },
    RUN_PROD_WORKFLOW_PLATFORM() {
        @Override
        public String getTestDescription() {
            return "Test will run a prod workflow for RUN_2_1_YAML_SANDBOX_CASE_1.";
        }

    },
    RUN_2_1_YAML_SANDBOX_CASE_2() {
        @Override
        public String getTestDescription() {
            return "Test will execute a 2.1 version Yaml via Sandbox CLI.\n"
                    + "Inputs: SMALL_CSV,LARGE_CSV,DEFAULT_DF \n"
                    + "Partitioned: False, Multi PipelineStep, Remote Script\n";
        }
    };

    public abstract String getTestDescription();

    public static String getInputFileName(DSPTestScenarioEnum dspTestScenarioEnum) throws TestScenarioExecutionException {
        if (dspTestScenarioEnum.equals(RUN_2_0_YAML_SANDBOX_CASE_1))
            return "/fixtures/dsp/test-cases/version_2_0_case_1.yaml";
        if (dspTestScenarioEnum.equals(RUN_2_0_YAML_SANDBOX_CASE_2))
            return "/fixtures/dsp/test-cases/version_2_0_case_2.yaml";
        if (dspTestScenarioEnum.equals(RUN_2_1_YAML_SANDBOX_CASE_1))
            return "/fixtures/dsp/test-cases/version_2_1_case_1.yaml";
        if (dspTestScenarioEnum.equals(RUN_2_1_YAML_SANDBOX_CASE_2))
            return "/fixtures/dsp/test-cases/version_2_1_case_2.yaml";
        if (dspTestScenarioEnum.equals(RUN_DRAFT_WORKFLOW_GROUP_PLATFORM) || dspTestScenarioEnum.equals(RUN_PROD_WORKFLOW_GROUP_PLATFORM))
            return "/fixtures/dsp/test-cases/runWorkflowGroupPayload.json";
        if (dspTestScenarioEnum.equals(RUN_DRAFT_WORKFLOW_PLATFORM)
                ||  dspTestScenarioEnum.equals(RUN_PROD_WORKFLOW_PLATFORM))
            return "/fixtures/dsp/test-cases/runWorkflowPayload.json";
        if (
                dspTestScenarioEnum.equals(PROMOTE_2_0_FLOW_ON_PLATFORM)
            ||
                dspTestScenarioEnum.equals(PROMOTE_2_1_FLOW_ON_PLATFORM))
            return "/fixtures/dsp/test-cases/promoteWorkflowPayload.json";
        else
            throw new TestScenarioExecutionException("No Input File For Test Scenario: " + dspTestScenarioEnum.name());
    }

    public static String getPlatformCallUrl(DSPTestScenarioEnum dspTestScenarioEnum) throws TestScenarioExecutionException {
        String workflowGroupName = "REGRESSION_SANDBOX_TEST_WFG";

        if (dspTestScenarioEnum.equals(RUN_DRAFT_WORKFLOW_GROUP_PLATFORM))
            return String.format(DSPConstants.RUN_WORKFLOW_GROUP_URL_FORMAT, DSPConstants.REGRESSION_HOST_NAME, workflowGroupName, true);
        if (dspTestScenarioEnum.equals(RUN_PROD_WORKFLOW_GROUP_PLATFORM))
            return String.format(DSPConstants.RUN_WORKFLOW_GROUP_URL_FORMAT, DSPConstants.REGRESSION_HOST_NAME, workflowGroupName, false);
        if (dspTestScenarioEnum.equals(RUN_DRAFT_WORKFLOW_PLATFORM)
                    || dspTestScenarioEnum.equals(RUN_PROD_WORKFLOW_PLATFORM))
            return String.format(DSPConstants.RUN_WORKFLOW_URL_FORMAT, DSPConstants.REGRESSION_HOST_NAME);
        else
            throw new TestScenarioExecutionException("Not a Platform Execution Test Scenario: " + dspTestScenarioEnum.name());
    }

    public static Object getRunPayload(String content, DSPTestScenarioEnum dspTestScenarioEnum) throws TestScenarioExecutionException{
        if (dspTestScenarioEnum.equals(RUN_DRAFT_WORKFLOW_GROUP_PLATFORM) || (dspTestScenarioEnum.equals(RUN_PROD_WORKFLOW_GROUP_PLATFORM)))
            return JsonUtils.DEFAULT.fromJson(content, WorkflowGroupExecuteRequest.class);

        if (dspTestScenarioEnum.equals(RUN_DRAFT_WORKFLOW_PLATFORM)
                || dspTestScenarioEnum.equals(RUN_PROD_WORKFLOW_PLATFORM))
            return JsonUtils.DEFAULT.fromJson(content, ExecuteWorkflowRequest.class);
        else
            throw new TestScenarioExecutionException("Not a Platform Execution Test Scenario: " + dspTestScenarioEnum.name());
    }

}

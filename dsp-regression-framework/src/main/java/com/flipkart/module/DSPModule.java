package com.flipkart.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.enums.DSPTestScenarioEnum;
import com.flipkart.testScenario.TestScenario;
import com.flipkart.testScenario.dsp.*;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DSPModule extends AbstractModule {

    @Inject private AzkabanConfig azkabanConfig;

    @Override
    protected void configure() {
        MapBinder<DSPTestScenarioEnum, TestScenario> mapbinder = MapBinder.newMapBinder(binder(), DSPTestScenarioEnum.class, TestScenario.class);
        mapbinder.addBinding(DSPTestScenarioEnum.CREATE_EXTERNAL_CREDENTIALS_SANDBOX).to(CreateCredentialsViaSandbox.class);
        mapbinder.addBinding(DSPTestScenarioEnum.RUN_2_0_YAML_SANDBOX_CASE_1).to(RunYAMLVersion20BySandbox.class);
        mapbinder.addBinding(DSPTestScenarioEnum.RUN_DRAFT_WORKFLOW_GROUP_PLATFORM).to(RunWorkflowOnPlatform.class);
        mapbinder.addBinding(DSPTestScenarioEnum.PROMOTE_2_0_FLOW_BY_SANDBOX).to(PromoteWorkFlowViaSandbox.class);
        mapbinder.addBinding(DSPTestScenarioEnum.PROMOTE_2_0_FLOW_ON_PLATFORM).to(PromoteWorkflowViaPlatform.class);
        mapbinder.addBinding(DSPTestScenarioEnum.RUN_PROD_WORKFLOW_GROUP_PLATFORM).to(RunWorkflowOnPlatform.class);
        mapbinder.addBinding(DSPTestScenarioEnum.RUN_2_0_YAML_SANDBOX_CASE_2).to(RunYAMLVersion20BySandbox.class);

        mapbinder.addBinding(DSPTestScenarioEnum.RUN_2_1_YAML_SANDBOX_CASE_1).to(RunYAMLVersion20BySandbox.class);
        mapbinder.addBinding(DSPTestScenarioEnum.RUN_DRAFT_WORKFLOW_PLATFORM).to(RunWorkflowOnPlatform.class);
        mapbinder.addBinding(DSPTestScenarioEnum.PROMOTE_2_1_FLOW_BY_SANDBOX).to(PromoteWorkFlowViaSandbox.class);
        mapbinder.addBinding(DSPTestScenarioEnum.PROMOTE_2_1_FLOW_ON_PLATFORM).to(PromoteWorkflowViaPlatform.class);
        mapbinder.addBinding(DSPTestScenarioEnum.RUN_PROD_WORKFLOW_PLATFORM).to(RunWorkflowOnPlatform.class);
        mapbinder.addBinding(DSPTestScenarioEnum.RUN_2_1_YAML_SANDBOX_CASE_2).to(RunYAMLVersion20BySandbox.class);
    }

    @Provides
    @Singleton
    ObjectMapper provideObjectMapper() {
        return new ObjectMapper();
    }

}

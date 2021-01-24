package com.flipkart.dsp.executor;

import com.flipkart.dsp.client.DSPConfigModule;
import com.flipkart.dsp.engine.config.ScriptExecutionModule;
import com.flipkart.dsp.executor.application.SignalShuffleApplication;
import com.flipkart.dsp.executor.application.WorkFlowMesosApplication;
import com.flipkart.dsp.executor.config.TestApplicationModule;
import com.flipkart.dsp.executor.exception.ApplicationException;
import com.flipkart.dsp.executor.module.ExecutorModule;
import com.flipkart.dsp.executor.module.LocationModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.dropwizard.testing.FixtureHelpers;

public class TestApplicationDriver {

  static final String frameworkId = "frameworkId";
  static final String slaveId = "slaveId";
  static final String hostIP = "hostIP";
  static final String executorId = "executorId";
  static final String cpus = "1";
  static final String mem = "1";
  static final String role = "stage";

  public static void main(String[] args) throws ApplicationException {
    String bucketName = "dsp-stage-beta";
    String[] payload = {FixtureHelpers.fixture("test_config_payload.json"), frameworkId,
            slaveId, hostIP, executorId, cpus, mem, role, "3", "2"};

    Injector injector = Guice.createInjector(Stage.DEVELOPMENT, new DSPConfigModule(bucketName), new LocationModule(), new ScriptExecutionModule());
    ExecutorModule executorModule = new ExecutorModule();
    injector.injectMembers(executorModule);
    injector = injector.createChildInjector(executorModule);
    WorkFlowMesosApplication workFlowMesosApplication = injector.getInstance(WorkFlowMesosApplication.class);
    workFlowMesosApplication.execute(payload);
//    SignalShuffleApplication signalShuffleApplication = injector.getInstance(SignalShuffleApplication.class);
//    signalShuffleApplication.execute(payload);
  }
}
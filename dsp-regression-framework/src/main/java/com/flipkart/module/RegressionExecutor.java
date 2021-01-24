package com.flipkart.module;

import com.flipkart.dsp.client.DSPConfigModule;
import com.flipkart.dto.DSPTestEnvVariables;
import com.flipkart.dto.TestEnvVariables;
import com.flipkart.team.Team;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;

public class RegressionExecutor {
    private Injector injector;

    private static  String DEFAULT_BUCKET = "dsp-regression";
    private static final String CONFIG_SERVICE_HOST = null;

    public <T extends Team> void execute(Class<T> applicationClass, TestEnvVariables args) throws Exception {

        if(args instanceof DSPTestEnvVariables) {
            if(((DSPTestEnvVariables) args).getBucketPostfix() != null) {
                DEFAULT_BUCKET = "dsp-" + ((DSPTestEnvVariables) args).getBucketPostfix();
            }
            this.injector = Guice.createInjector(Stage.DEVELOPMENT, new DSPConfigModule(DEFAULT_BUCKET, CONFIG_SERVICE_HOST));
            DSPModule dspModule = new DSPModule();
            injector.injectMembers(dspModule);
            injector = injector.createChildInjector(dspModule);
        }
        T application = injector.getInstance(applicationClass);
        application.performRegressionTest(args);
    }
}

package com.flipkart.dsp.executor.executor;

import com.flipkart.dsp.client.DSPConfigModule;
import com.flipkart.dsp.engine.config.ScriptExecutionModule;
import com.flipkart.dsp.executor.application.AbstractApplication;
import com.flipkart.dsp.executor.exception.ApplicationException;
import com.flipkart.dsp.executor.module.ExecutorModule;
import com.flipkart.dsp.executor.module.LocationModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ApplicationExecutor {
    private Injector injector;

    public ApplicationExecutor(String configBucket, String configServiceHost) {
        this.injector = Guice.createInjector(Stage.DEVELOPMENT, new DSPConfigModule(configBucket, configServiceHost), new LocationModule(), new ScriptExecutionModule());
        ExecutorModule executorModule = new ExecutorModule();
        injector.injectMembers(executorModule);
        injector = injector.createChildInjector(executorModule);
    }

    public <T extends AbstractApplication> void execute(Class<T> applicationClass, String[] args) throws ApplicationException {
        T application = injector.getInstance(applicationClass);
        application.execute(args);
    }
}

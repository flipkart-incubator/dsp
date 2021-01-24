package com.flipkart.dsp.jobs;

import com.flipkart.dsp.client.DSPConfigModule;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.module.AzkabanCommonModule;
import com.flipkart.dsp.utils.NodeMetaData;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import lombok.extern.slf4j.Slf4j;

import static com.google.inject.Stage.PRODUCTION;

/**
 */
@Slf4j
public class SessionfulApplicationExecutor {

    private static Injector injector;

    private SessionfulApplicationExecutor() { }

    public static <T extends SessionfulApplication> NodeMetaData execute(Class<T> applicationClass,
                                                                         Class abstractModuleClass,
                                                                         String[] args, String azkabanHostIp)
            throws AzkabanException, InstantiationException, IllegalAccessException {
        T application = getInstance(applicationClass, abstractModuleClass, azkabanHostIp);
        return application.execute(args);
    }

    public static <T> T getInstance(Class<T> type, Class abstractModuleClass, String azkabanHostIp) throws IllegalAccessException, InstantiationException {
        if (injector == null) {
            injector = Guice.createInjector(PRODUCTION, new DSPConfigModule(null, azkabanHostIp));
            AzkabanCommonModule azkabanCommonModule = new AzkabanCommonModule();
            injector.injectMembers(azkabanCommonModule);
            AbstractModule abstractModule = (AbstractModule) abstractModuleClass.newInstance();
            injector.injectMembers(abstractModule);
            injector = injector.createChildInjector(azkabanCommonModule, abstractModule);
        }
        return injector.getInstance(type);
    }
}

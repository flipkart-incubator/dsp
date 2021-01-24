package com.flipkart.dsp.module;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.cosmos.AzkabanCosmosTag;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.service.AzkabanWorkflowExecutionService;
import com.flipkart.dsp.service.WorkflowExecutionService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.palominolabs.metrics.guice.MetricsInstrumentationModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.flipkart.dsp.utils.Constants.DEFAULT_FILE_SYSTEM;
import static com.flipkart.dsp.utils.Constants.HADOOP_USER_PROPERTY;

/**
 */
@Slf4j
public class AzkabanModule extends AbstractModule {

    @Inject
    private DSPServiceConfig dspServiceConfig;

    private CosmosReporter cosmosReporter;

    @Override
    protected void configure() {
        MetricRegistry metricRegistry = new MetricRegistry();
        cosmosReporter = CosmosReporter.create(metricRegistry, getLogger(), new AzkabanCosmosTag(),
                TimeUnit.MICROSECONDS, false);
        cosmosReporter.start(dspServiceConfig.getAzkabanConfig().getCosmosInterval(), TimeUnit.MILLISECONDS);
        install(MetricsInstrumentationModule.builder().withMetricRegistry(metricRegistry).build());
        bind(WorkflowExecutionService.class).to(AzkabanWorkflowExecutionService.class);
    }

    @Provides
    @Singleton
    CosmosReporter provideCustomReporter() {
        return cosmosReporter;
    }

    @Provides
    @Singleton
    Logger getLogger() {
        return LoggerFactory.getLogger("cosmoslogger");
    }


    @Provides
    @Singleton
    public DSPServiceConfig.MesosConfig providesMesosConfig() {
        return dspServiceConfig.getMesosConfig();
    }

    @Provides
    @Singleton
    public FileSystem provideHDFSFileSystem() throws IOException, InterruptedException {
        String hdfsUser = dspServiceConfig.getHadoopConfig().getUser();
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FILE_SYSTEM, dspServiceConfig.getHadoopConfig().getHostUrl());
        conf.set(HADOOP_USER_PROPERTY, hdfsUser);
        AtomicReference<FileSystem> fileSystemAtomicReference = new AtomicReference<>();
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            FileSystem fs = FileSystem.get(conf);
            fileSystemAtomicReference.set(fs);
            return null;
        });
        return fileSystemAtomicReference.get();
    }
}

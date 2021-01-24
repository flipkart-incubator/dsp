package com.flipkart.dsp.executor.module;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.executor.cosmos.MesosCosmosTag;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.palominolabs.metrics.guice.MetricsInstrumentationModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.flipkart.dsp.utils.Constants.DEFAULT_FILE_SYSTEM;
import static com.flipkart.dsp.utils.Constants.HADOOP_USER_PROPERTY;

@Slf4j
@RequiredArgsConstructor
public class ExecutorModule extends AbstractModule {
    private CosmosReporter cosmosReporter;

    @Inject
    private DSPServiceConfig dspServiceConfig;

    @Override
    protected void configure() {
        MetricRegistry metricRegistry = new MetricRegistry();
        cosmosReporter = CosmosReporter.create(metricRegistry, provideLogger(), new MesosCosmosTag(),
                TimeUnit.MICROSECONDS, true);
        cosmosReporter.start(dspServiceConfig.getMesosConfig().getCosmosMesosInterval(), TimeUnit.MILLISECONDS);
        install(MetricsInstrumentationModule.builder().withMetricRegistry(metricRegistry).build());
    }

    @Provides
    @Singleton
    ObjectMapper provideObjectMapper() {
        return new ObjectMapper();
    }

    @Provides
    @Singleton
    CosmosReporter provideCustomReporter() {
        return cosmosReporter;
    }

    @Provides
    @Singleton
    Logger provideLogger() {
        return LoggerFactory.getLogger("cosmoslogger");
    }


    @Provides
    @Singleton
    public FileSystem provideHDFSFileSystem(HadoopConfig hadoopConfig) throws IOException, InterruptedException {
        String hdfsUser = hadoopConfig.getUser();
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FILE_SYSTEM, hadoopConfig.getHostUrl());
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

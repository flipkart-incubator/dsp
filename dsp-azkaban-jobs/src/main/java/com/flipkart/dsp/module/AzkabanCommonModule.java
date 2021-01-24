package com.flipkart.dsp.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.dsp.config.*;
import com.flipkart.dsp.dao.core.SessionFactoryInitializer;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.entity.HiveConfigParam;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jackson.Jackson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;

import javax.inject.Inject;


@Slf4j
@RequiredArgsConstructor
public class AzkabanCommonModule extends AbstractModule {

    @Inject
    private DSPServiceConfig dspServiceConfig;

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    public ObjectMapper providesObjectMapper() {
        return Jackson.newObjectMapper();
    }


    @Provides
    @Singleton
    DataSourceFactory makeServiceDatabaseConfig() {
        return dspServiceConfig.getDatabase();
    }


    @Provides
    @Singleton
    public SessionFactory provideServiceSessionFactory() {
        return new SessionFactoryInitializer(dspServiceConfig).getSessionFactory();
    }

    @Provides
    @Singleton
    public HiveClient providesHiveClient() {
        HiveConfig hiveConfig = dspServiceConfig.getHiveConfig();
        DSPClientConfig dspClientConfig = dspServiceConfig.getDspClientConfig();
        String url = hiveConfig.getUrl();

        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, hiveConfig.getUser(), hiveConfig.getPassword(),
                hiveConfig.getConnectionPoolSize(), dspClientConfig.getRetryGapInMillis(),
                dspClientConfig.getMaxRetries(), hiveConfig.getMaxIdleConnections() == null ? 0 :
                hiveConfig.getMaxIdleConnections());

        return new HiveClient(hiveConfigParam);
    }


}

package com.flipkart.dsp.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.config.JerseyResourceModule;
import com.flipkart.dsp.db.entities.SignalEntity;
import com.flipkart.dsp.db.entities.WorkflowEntity;
import com.flipkart.dsp.exceptions.mapper.EntityNotFoundExceptionMapper;
import com.flipkart.dsp.resources.WorkFlowResource;
import com.hubspot.dropwizard.guicier.GuiceBundle;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.ScanningHibernateBundle;
import io.dropwizard.hibernate.SessionFactoryFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.jersey.server.ServerProperties;
import org.reflections.Reflections;

import javax.ws.rs.ext.ExceptionMapper;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 */
@Slf4j
public class DSPApplication extends Application<DSPServiceConfig> {

    public static void main(String[] args) throws Exception {
        new DSPApplication().run(args);
    }

    @Override
    public void run(DSPServiceConfig configuration, Environment environment) {
        registerExceptionMappers(environment);
        environment.jersey().property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
        environment.getObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    private void registerExceptionMappers(Environment environment) {
        List<String> mapperPackages = asList(com.flipkart.dsp.exception.mappers.ExceptionMapper.class.getPackage().getName()
                , EntityNotFoundExceptionMapper.class.getPackage().getName());

        mapperPackages.stream().forEach(pkg -> {
            Reflections reflections = new Reflections(pkg);
            Set<Class<? extends ExceptionMapper>>
                    mappers =
                    reflections.getSubTypesOf(ExceptionMapper.class);
            mappers.stream().forEach(c -> environment.jersey().register(c));
        });
    }

    @Override
    public void initialize(Bootstrap<DSPServiceConfig> bootstrap) {
        String resourcePackage = WorkFlowResource.class.getPackage().getName();

        HibernateBundle<DSPServiceConfig> hibernateBundle = buildHibernateBundle();

        JerseyResourceModule<DSPServiceConfig>
                resourceModule =
                new JerseyResourceModule<>(resourcePackage);

        GuiceBundle<DSPServiceConfig>
                guiceBundle =
                GuiceBundle.defaultBuilder(DSPServiceConfig.class).enableGuiceEnforcer(false)
                        .modules(new DSPModule(hibernateBundle, bootstrap.getObjectMapper(),
                                bootstrap.getMetricRegistry()), resourceModule).build();

        bootstrap.addBundle(hibernateBundle);
        bootstrap.addBundle(guiceBundle);

        bootstrap.addBundle(new SwaggerBundle<Configuration>() {

            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(Configuration configuration) {
                final SwaggerBundleConfiguration swaggerBundleConfiguration = new SwaggerBundleConfiguration();
                swaggerBundleConfiguration.setResourcePackage(resourcePackage);
                return swaggerBundleConfiguration;
            }
        });

    }

    private HibernateBundle<DSPServiceConfig> buildHibernateBundle() {
        String[] packages = new String[]{WorkflowEntity.class.getPackage().getName(), SignalEntity.class.getPackage().getName()};
        return new ScanningHibernateBundle<DSPServiceConfig>(packages, new SessionFactoryFactory()) {
            @Override
            public PooledDataSourceFactory getDataSourceFactory(
                    DSPServiceConfig dspServiceConfig) {
                return dspServiceConfig.getDatabase();
            }
        };
    }
}

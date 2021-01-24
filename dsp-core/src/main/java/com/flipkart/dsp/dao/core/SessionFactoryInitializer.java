package com.flipkart.dsp.dao.core;

import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.dropwizard.db.DataSourceFactory;
import org.apache.tomcat.dbcp.dbcp.DriverManagerConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.PoolableConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.PoolingDataSource;
import org.apache.tomcat.dbcp.pool.impl.GenericObjectPool;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.connections.internal.DatasourceConnectionProviderImpl;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.ServiceRegistry;
import org.reflections.Reflections;

import javax.persistence.Entity;
import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;

/**
 * Leveraging the code / logic mostly from dropwizard-hibernate's
 *  SessionFactoryFactory
 *  ManagedPooledDataSource
 */
@Singleton
public final class SessionFactoryInitializer {

    private final SessionFactory sessionFactory;
    private DataSource dataSource;

    /**
     * @param dspServiceConfig
     */
    @Inject
    public SessionFactoryInitializer(DSPServiceConfig dspServiceConfig) {
        final Configuration configuration = new Configuration();
        configuration.setProperty(AvailableSettings.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        configuration.setProperty(AvailableSettings.USE_SQL_COMMENTS, dspServiceConfig.getDatabase().isAutoCommentsEnabled() + "");
        configuration.setProperty(AvailableSettings.USE_GET_GENERATED_KEYS, "true");
        configuration.setProperty(AvailableSettings.GENERATE_STATISTICS, "true");
        configuration.setProperty(AvailableSettings.USE_REFLECTION_OPTIMIZER, "true");
        configuration.setProperty(AvailableSettings.ORDER_UPDATES, "true");
        configuration.setProperty(AvailableSettings.ORDER_INSERTS, "true");
        configuration.setProperty(AvailableSettings.USE_NEW_ID_GENERATOR_MAPPINGS, "true");
        configuration.setProperty(AvailableSettings.SHOW_SQL, "false");
        configuration.setProperty(AvailableSettings.C3P0_MIN_SIZE, "5");
        configuration.setProperty(AvailableSettings.C3P0_MAX_SIZE, "10");
        configuration.setProperty(AvailableSettings.FORMAT_SQL, "true");
        configuration.setProperty(AvailableSettings.USE_SQL_COMMENTS, "true");
        configuration.setProperty(AvailableSettings.ORDER_INSERTS, "true");
        configuration.setProperty(AvailableSettings.ORDER_UPDATES, "true");
        configuration.setProperty("jadira.usertype.autoRegisterUserTypes", "true");

        addAnnotatedClasses(configuration);

        final ServiceRegistry registry = new StandardServiceRegistryBuilder()
                .addService(ConnectionProvider.class, buildConnectionProvider(dspServiceConfig))
                .applySettings(configuration.getProperties()).build();

        this.sessionFactory = configuration.buildSessionFactory(registry);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            sessionFactory.close();
        }));
    }

    private void addAnnotatedClasses(Configuration configuration) {
        Set<Class<?>> entities = new Reflections("com.flipkart.dsp.db.entities").getTypesAnnotatedWith(Entity.class);
        final SortedSet<String> entityClasses = Sets.newTreeSet();
        for (Class<?> klass : entities) {
            configuration.addAnnotatedClass(klass);
            entityClasses.add(klass.getCanonicalName());
        }
    }

    private ConnectionProvider buildConnectionProvider(DSPServiceConfig dspServiceConfig) {
        DataSourceFactory dataSourceFactory = dspServiceConfig.getDatabase();

        final Properties properties = new Properties();
        for (Map.Entry<String, String> property : dataSourceFactory.getProperties().entrySet()) {
            properties.setProperty(property.getKey(),  property.getValue());
        }

        properties.setProperty("user", dataSourceFactory.getUser());
        properties.setProperty("password", dataSourceFactory.getPassword());

        final DriverManagerConnectionFactory factory = new DriverManagerConnectionFactory(
                dataSourceFactory.getUrl(),
                properties);
        GenericObjectPool pool = buildPool(dspServiceConfig);

        PoolableConnectionFactory connectionFactory = new PoolableConnectionFactory(factory,
                pool,
                null,
                dataSourceFactory.getValidationQuery(),
                ImmutableList.of(),
                dataSourceFactory.isDefaultReadOnly(),
                true);

        this.dataSource = new PoolingDataSource(connectionFactory.getPool());

        final DatasourceConnectionProviderImpl connectionProvider = new DatasourceConnectionProviderImpl();
        connectionProvider.setDataSource(dataSource);
        connectionProvider.configure(properties);
        return connectionProvider;
    }

    private GenericObjectPool buildPool(DSPServiceConfig dspServiceConfig) {
        DataSourceFactory dataSourceFactory = dspServiceConfig.getDatabase();
        AzkabanConfig azkabanConfig = dspServiceConfig.getAzkabanConfig();
        final GenericObjectPool pool = new GenericObjectPool(null);

        pool.setMaxWait(dataSourceFactory.getMaxWaitForConnection().toSeconds());
        pool.setMinIdle(dataSourceFactory.getMinSize());
        pool.setMaxActive(azkabanConfig.getMaxDbConnection());
        pool.setMaxIdle(azkabanConfig.getMaxDbConnection());
        pool.setTestWhileIdle(dataSourceFactory.getCheckConnectionWhileIdle());
        pool.setTimeBetweenEvictionRunsMillis(dataSourceFactory.getEvictionInterval().toMilliseconds());
        pool.setMinEvictableIdleTimeMillis(dataSourceFactory.getMinIdleTime().toMilliseconds());
        pool.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);

        return pool;
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

}

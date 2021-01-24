package com.flipkart.dsp.service;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.dsp.config.*;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.entity.HiveConfigParam;
import com.google.inject.*;
import com.google.inject.matcher.Matchers;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWork;
import io.dropwizard.hibernate.UnitOfWorkAspect;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.HttpClient;
import org.hibernate.SessionFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static com.flipkart.dsp.utils.Constants.*;

/**
 */
@Slf4j
public class DSPModule extends AbstractModule {
    private MetricRegistry metricRegistry;
    private final ObjectMapper objectMapper;
    private final HibernateBundle<DSPServiceConfig> hibernateBundle;

    public DSPModule(HibernateBundle<DSPServiceConfig> hibernateBundle,
                     ObjectMapper objectMapper, MetricRegistry metricRegistry) {
        this.hibernateBundle = hibernateBundle;
        this.objectMapper = objectMapper;
        this.metricRegistry = metricRegistry;
    }

    @Override
    protected void configure() {
        UnitOfWorkInterceptor interceptor = new UnitOfWorkInterceptor();
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(UnitOfWork.class), interceptor);
        requestInjection(interceptor);
    }

    @Provides
    @Singleton
    private Client provideJerseyClient() {
        try {
            SSLContext sslcontext = SSLContext.getInstance("TLS");
            sslcontext.init(null, new TrustManager[]{new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                }

                public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                }

                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

            }}, new java.security.SecureRandom());

            return ClientBuilder.newBuilder().sslContext(sslcontext).hostnameVerifier((requestedHost, remoteServerSession) -> true).build();

        } catch (KeyManagementException | NoSuchAlgorithmException ex) {
            throw new RuntimeException("Unable to initialize JerseyClient!", ex);
        }
    }

    @Provides
    MetricRegistry providesMetricRegistry() {
        return metricRegistry;
    }

    @Provides
    @Singleton
    public GithubConfig provideGithubConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getGithubConfig();
    }

    @Provides
    @Singleton
    SessionFactory makeSessionFactory() {
        return hibernateBundle.getSessionFactory();
    }

    @Provides
    @Singleton
    AzkabanConfig provideAzkabanConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getAzkabanConfig();
    }


    @Provides
    @Singleton
    DSPServiceConfig.HDFSConfig provideHdfsConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getHdfsConfig();
    }

    @Provides
    @Singleton
    HiveConfig provideHiveConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getHiveConfig();
    }


    @Provides
    @Singleton
    public HiveClient providesHiveClient(DSPServiceConfig dspServiceConfig) {
        HiveConfig hiveConfig = dspServiceConfig.getHiveConfig();
        HiveConfigParam hiveConfigParam = new HiveConfigParam(hiveConfig.getUrl(), hiveConfig.getUser(), hiveConfig.getPassword(),
                        hiveConfig.getConnectionPoolSize(), hiveConfig.getRetryGapInMillis(),
                        hiveConfig.getMaxRetries(), hiveConfig.getMaxIdleConnections() == null ? 0 :
                        hiveConfig.getMaxIdleConnections());

        return new HiveClient(hiveConfigParam);
    }

    @Provides
    @Singleton
    public MetaStoreClient providesMetaStoreClient(DSPServiceConfig dspServiceConfig) throws MetaException {
        HiveConfig hiveConfig = dspServiceConfig.getHiveConfig();
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveConfig.getMetaStoreURI());
        hiveConf.setVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, "3");
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME, "3600");
        HiveMetaStoreClient hiveMetaStoreClient=new HiveMetaStoreClient(hiveConf);
        return new MetaStoreClient(hiveMetaStoreClient);
    }

    @Provides
    @Singleton
    DSPClientConfig providesDSPClientConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getDspClientConfig();
    }

    @Provides
    @Singleton
    ObjectMapper provideObjectMapper() {
        return objectMapper;
    }

    @Provides
    @Singleton
    public ExecutorService providesExecutorService(DSPServiceConfig dspServiceConfig) {
        return Executors.newFixedThreadPool(dspServiceConfig.getExecutorLogsConfig().getThreadPoolSize());
    }

    @Provides
    @Singleton
    public HttpClient providesHtpClient(Provider<Environment> environmentProvider, DSPServiceConfig dspServiceConfiguration) {
        return new HttpClientBuilder(environmentProvider.get()).using(dspServiceConfiguration.getHttpClientConfiguration()).build("");
    }

    @Provides
    @Singleton
    UnitOfWorkAwareProxyFactory provideUnitOfWorkAwareProxyFactory() {
        return new UnitOfWorkAwareProxyFactory(this.hibernateBundle);
    }

    private static class UnitOfWorkInterceptor implements MethodInterceptor {
        @Inject
        UnitOfWorkAwareProxyFactory proxyFactory;

        @Override
        public Object invoke(MethodInvocation methodInvocation) throws Throwable {
            UnitOfWorkAspect aspect = proxyFactory.newAspect();
            try {
                aspect.beforeStart(methodInvocation.getMethod().getAnnotation(UnitOfWork.class));
                Object result = methodInvocation.proceed();
                aspect.afterEnd();
                return result;
            } catch (InvocationTargetException e) {
                aspect.onError();
                throw e.getCause();
            } catch (Exception e) {
                aspect.onError();
                throw e;
            } finally {
                aspect.onFinish();
            }
        }
    }

    @Provides
    @Singleton
    public FileSystem provideHDFSFileSystem(DSPServiceConfig dspServiceConfig) throws IOException, InterruptedException {
        HadoopConfig hadoopConfig = dspServiceConfig.getHadoopConfig();
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

    @Provides
    @Singleton
    MiscConfig provideMiscConfig(DSPServiceConfig serviceConfig) {
        return serviceConfig.getMiscConfig();
    }
    @Provides
    @Singleton
    public DSPServiceConfig.CosmosConfig providesCosmosConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getCosmosConfig();
    }
}


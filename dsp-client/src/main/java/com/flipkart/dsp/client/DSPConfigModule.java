package com.flipkart.dsp.client;

import com.flipkart.dsp.config.*;
import com.flipkart.dsp.qe.entity.HiveConfigParam;
import com.flipkart.dsp.utils.*;
import com.google.inject.*;
import io.dropwizard.db.DataSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.Map;
import java.util.concurrent.*;

/**
 */
@Slf4j
public class DSPConfigModule extends AbstractModule {
    private String bucket;
    private String configServiceHost;


    public DSPConfigModule(String bucket) {
        this.bucket = bucket;
    }

    public DSPConfigModule(String bucket, String configServiceHost) {
        this.bucket = bucket;
        this.configServiceHost = configServiceHost;
    }

    @Override
    protected void configure() {
    }


    @Provides
    @Singleton
    public DSPServiceConfig getDSPServiceConfig() {
        int attemps = 0;
        int MAX_ATTEMPTS = 5;
        long currentDelay = 3; //seconds
        while (true) {
            try {
                String bucketName = System.getProperty(Constants.CONFIG_SVC_BUCKETS_KEY);
                if (bucketName == null || bucketName.isEmpty()) {
                    bucketName = bucket;
                }

                log.info("ConfigService Bucket => {}", bucketName);
                
                log.info("Fetching config from config service sidekick. . .");
                HttpRequestClient httpRequestClient = new HttpRequestClient(JsonUtils.DEFAULT.mapper);
                ConfigServiceSidekickClient configServiceSidekickClient = new ConfigServiceSidekickClient(httpRequestClient);
                Map<String, Object> configMap = configServiceSidekickClient.getConfigBucketForSidekick(configServiceHost, bucketName);

                return parseDSPServiceConfig(configMap);
            } catch (Exception e) {
                log.error("Exception when initialising DynamicBucket", e);
                if (attemps != MAX_ATTEMPTS) {
                    attemps++;
                    currentDelay *= 2;
                    try {
                        TimeUnit.SECONDS.sleep(currentDelay);
                    } catch (InterruptedException e1) {
                        log.warn("Got interrupted while waiting for retry, ignoring", e); //there is nothing we can do to ease the pain.
                    }
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private DSPServiceConfig parseDSPServiceConfig(Map<String, Object> configMap) {
        log.info("Initialising DSP Service Config");
        try {
            DSPServiceConfig dspServiceConfig = ConfigServiceUtils.getConfig(configMap, "dspServiceConfig", DSPServiceConfig.class);
            dspServiceConfig.setHiveConfig(ConfigServiceUtils.getConfig(configMap, "hiveConfig", HiveConfig.class));
            dspServiceConfig.setMiscConfig(ConfigServiceUtils.getConfig(configMap, "miscConfig", MiscConfig.class));
            dspServiceConfig.setCosmosConfig(ConfigServiceUtils.getConfig(configMap, "cosmosConfig", DSPServiceConfig.CosmosConfig.class));
            dspServiceConfig.setHdfsConfig(ConfigServiceUtils.getConfig(configMap, "hdfsConfig", DSPServiceConfig.HDFSConfig.class));
            dspServiceConfig.setMesosConfig(ConfigServiceUtils.getConfig(configMap, "mesosConfig", DSPServiceConfig.MesosConfig.class));
            dspServiceConfig.setHadoopConfig(ConfigServiceUtils.getConfig(configMap, "hadoopConfig", HadoopConfig.class));
            dspServiceConfig.setGithubConfig(ConfigServiceUtils.getConfig(configMap, "githubConfig", GithubConfig.class));
            dspServiceConfig.setDatabase(ConfigServiceUtils.getConfig(configMap, "database", DataSourceFactory.class));
            dspServiceConfig.setAzkabanConfig(ConfigServiceUtils.getConfig(configMap, "azkabanConfig", AzkabanConfig.class));
            dspServiceConfig.setDspClientConfig(ConfigServiceUtils.getConfig(configMap, "dspClientConfig", DSPClientConfig.class));
            dspServiceConfig.setImageSnapShotConfig(ConfigServiceUtils.getConfig(configMap, "imageSnapShotConfig", DSPServiceConfig.ImageSnapShotConfig.class));
            dspServiceConfig.setScriptExecutionConfig(ConfigServiceUtils.getConfig(configMap, "scriptExecutionConfig", DSPServiceConfig.ScriptExecutionConfig.class));
            return dspServiceConfig;
        } catch (Exception e) {
            log.error("Exception while creating DSPClientConfig object", e);
            throw new RuntimeException(e);
        }
    }

    @Provides
    @Singleton
    public DSPServiceClient provideDSPServiceClient(DSPServiceConfig dspServiceConfig) {
        DSPClientConfig dspClientConfig = dspServiceConfig.getDspClientConfig();
        DSPServiceConfig.ScriptExecutionConfig scriptExecutionConfig = dspServiceConfig.getScriptExecutionConfig();
        return new DSPServiceClient(dspClientConfig.getHost(), dspClientConfig.getPort(), dspClientConfig.getMaxRetries(),
                dspClientConfig.getRetryGapInMillis(), scriptExecutionConfig.getWorkingDir(), dspClientConfig.getRequestTimeout());
    }

    @Provides
    @Singleton
    public HiveConfigParam provideHiveConfigParam(DSPServiceConfig dspServiceConfig) {
        HiveConfig hiveConfig = dspServiceConfig.getHiveConfig();
        return new HiveConfigParam(hiveConfig.getUrl(), hiveConfig.getUser(), hiveConfig.getPassword(),
                hiveConfig.getConnectionPoolSize(), hiveConfig.getRetryGapInMillis(),
                hiveConfig.getMaxRetries(), hiveConfig.getMaxIdleConnections());

    }

    @Provides
    @Singleton
    public HiveConf getHiveConfig(DSPServiceConfig dspServiceConfig) {
        String url = dspServiceConfig.getHiveConfig().getMetaStoreURI();
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, url);
        return hiveConf;
    }

    @Provides
    @Singleton
    public HiveMetaStoreClient provideHiveMetaStoreClient(HiveConf hiveConf) throws MetaException {
        hiveConf.setVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, "3");
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME, "3600");
        return new HiveMetaStoreClient(hiveConf);
    }


    @Provides
    @Singleton
    public ExecutorService providesExecutionService() {
        return Executors.newWorkStealingPool();
    }

    @Provides
    @Singleton
    public MiscConfig provideMiscConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getMiscConfig();
    }

    @Provides
    @Singleton
    public HiveConfig provideHiveConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getHiveConfig();
    }

    @Provides
    @Singleton
    HadoopConfig provideHadoopConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getHadoopConfig();
    }

    @Provides
    @Singleton
    DSPServiceConfig.ScriptExecutionConfig provideScriptExecutionConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getScriptExecutionConfig();
    }

    @Provides
    @Singleton
    DSPClientConfig provideDSPClientConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getDspClientConfig();
    }

    @Provides
    @Singleton
    public AzkabanConfig providesAzkabanConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getAzkabanConfig();
    }

    @Provides
    @Singleton
    public GithubConfig providesGithubConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getGithubConfig();
    }

    @Provides
    @Singleton
    public DSPServiceConfig.CosmosConfig providesCosmosConfig(DSPServiceConfig dspServiceConfig) {
        return dspServiceConfig.getCosmosConfig();
    }
}

package com.flipkart.dsp.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.flipkart.dsp.utils.StringToHashMapDeserializer;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.db.DataSourceFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class DSPServiceConfig extends Configuration {
    private HiveConfig hiveConfig;
    private MiscConfig miscConfig;
    private HDFSConfig hdfsConfig;
    private MesosConfig mesosConfig;
    private CosmosConfig cosmosConfig;
    private HadoopConfig hadoopConfig;
    private GithubConfig githubConfig;
    private DataSourceFactory database;
    private AzkabanConfig azkabanConfig;
    private DSPClientConfig dspClientConfig;
    private ScriptExecutionConfig scriptExecutionConfig;
    private ExecutorLogsConfig executorLogsConfig = new ExecutorLogsConfig();
    private ImageSnapShotConfig imageSnapShotConfig= new ImageSnapShotConfig();
    private HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();

    @Data
    @NoArgsConstructor
    public static class ImageSnapShotConfig {
        private double cpus = 1;
        private int retires = 3;
        private double memory = 2048;
    }


    @Data
    public static class MesosConfig {
        private Integer maxAgentCpu;
        private Integer maxAgentMemory;
        private String zookeeperAddress;
        private int cosmosMesosInterval = 100000;

        //<hostLocation, containerLocation>
        @JsonDeserialize(using = StringToHashMapDeserializer.class)
        private Map<String,String> mountInfo;
        @JsonDeserialize(using = StringToHashMapDeserializer.class)
        private Map<String,String> containerOptions;
    }

    @Data
    public static class ScriptExecutionConfig {
        private String workingDir = "/tmp";
        private boolean debugMode = false;
        private boolean redirectJRIConsoleToStdOut = true;
        private String rLibraryPath = "/Library/Frameworks/R.framework/Resources/library/rJava/jri/";
        private String sgDockerImage = "0.0.0.0/bullseye-java:test-1.1";
    }

    @Data
    public static class HDFSConfig {
        private String port = "50070";
        private String activeNameNode = "0.0.0.0";
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExecutorLogsConfig {
        private Integer threadPoolSize = 1024;
        private Integer waitInSeconds = 5;
    }

    @Data
    public static class CosmosConfig {
        private String cosmosEndPoint;
        private String mesosAppId;
        private String cosmosScriptName;
    }

}

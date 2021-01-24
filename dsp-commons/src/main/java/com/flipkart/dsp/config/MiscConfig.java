package com.flipkart.dsp.config;

import lombok.Data;


/**
 */
@Data
public class MiscConfig {
    private String saltKey;
    private String environment;
    private String bucketPostfix;
    private String ftpBaseHDFSPath;
    private String cephBaseHDFSPath;
    private String serviceOorFile;
    private String executorJarVersion;
    private String dataBasePath = "/tmp";
    private String tempDir = "/tmp/dsp-svc";
    private String localBasePath = "/tmp/dsp-service/";
    private String defaultNotificationEmailId = "dsp-notifications@flipkart.com";
    private String blobBasePath = "/projects/planning/dsp_stage/blob";
}

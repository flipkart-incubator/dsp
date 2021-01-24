package com.flipkart.dsp.config;

import lombok.Data;

/**
 * +
 */
@Data
public class HadoopConfig {
    private String basePath;
    private String hostUrl = "hdfs://hadoopcluster2";
    private String user = "fk-ip-data-service";
    private Double csvFileSizeThreshold = 10485760D; // 10 mb
    private String modelRepoLocation = "/projects/planning/dsp_stage/model_repo";
}

package com.flipkart.dsp.config;

import lombok.Data;

/**
 * +
 */
@Data
public class HiveConfig {
    private String url;
    private String user;
    private String sgDatabase;
    private String password = "";
    private String metaStoreURI;
    private Integer maxRetries = 5;
    private Integer connectionPoolSize;
    private Integer maxIdleConnections = 0;
    private Integer retryGapInMillis = 1000;
}

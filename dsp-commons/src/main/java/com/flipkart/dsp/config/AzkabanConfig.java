package com.flipkart.dsp.config;

import lombok.Data;

/**
 * +
 */
@Data
public class AzkabanConfig {
    private String user;
    private String host;
    private Integer port;
    private String password;
    private String jarVersion;
    private String elbEndPoint;
    private Double requestTimeOut;
    private int cosmosInterval = 10000;
    private Integer maxDbConnection = 1;
}



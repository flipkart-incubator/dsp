package com.flipkart.dsp.qe.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 */

@Getter
@Setter
@RequiredArgsConstructor
@AllArgsConstructor
public class HiveConfigParam {
    private String url;
    private String user;
    private String password;
    private Integer connectionPoolSize;
    private Integer retryGapInMillis=1000;
    private Integer maxRetries=3;
    private Integer maxIdleConnections = 0;
}

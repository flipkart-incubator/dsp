package com.flipkart.dsp.config;

import lombok.Data;

import java.io.Serializable;

/**
 * +
 */
@Data
public class DSPClientConfig implements Serializable {
    private int port = 9090;
    private int maxRetries = 4;
    private String host = "localhost";
    private int retryGapInMillis = 1000;
    private int requestTimeout = 500000;
    private String clientId = "IPP.DSP-STAGE";
}

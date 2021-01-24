package com.flipkart.dsp.service;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public class Run {

    public static final String CONFIG_SVC_BUCKETS_ENV_KEY = "DSP_SVC_CONFIG_BUCKETS";

    public static void main(String[] args) throws Exception {
        String resourcePath;
        if (args.length >= 1 && StringUtils.endsWith(args[0], ".json")) {
            resourcePath = args[0].trim();
        } else {
//          String configSvcBucket = System.getenv(CONFIG_SVC_BUCKETS_ENV_KEY);
            String configSvcBucket = "config-svc://0.0.0.0:80/dsp-stage-beta";
            if (configSvcBucket == null) {
                throw new IllegalStateException("Config Svc Bucket not provided !!!");
            }
            log.info("Config service bucket: {}", configSvcBucket);
            resourcePath = configSvcBucket;
        }
        new DSPApplication().run(new String[]{"server", resourcePath});
    }
}

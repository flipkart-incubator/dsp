package com.flipkart.dsp.qe.utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetryWaitLogic {
    private static final int RETRY_EXPONENTIAL_FACTOR = 2;

    public static int backOffAndWait(int retryGapInMillis) {
        try {
            log.info("Waiting for " + retryGapInMillis + " ms ......");
            Thread.sleep(retryGapInMillis);
            retryGapInMillis = retryGapInMillis * RETRY_EXPONENTIAL_FACTOR;
        } catch (InterruptedException e) {
            log.error("Exception while waiting for next retry", e);
        }
        return retryGapInMillis;
    }
}

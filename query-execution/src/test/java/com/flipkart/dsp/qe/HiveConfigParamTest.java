package com.flipkart.dsp.qe;

import com.flipkart.dsp.qe.entity.HiveConfigParam;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class HiveConfigParamTest {

    @Test
    public void testHiveConfigParam(){
        String url = "localhost.com";
        String user = "abcd";
        String password = "123";
        Integer poolSize = 10;
        Integer retryGap = 10;
        Integer maxRetry = 2;
        Integer maxIdleConnections = 0;
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, password, poolSize, retryGap, maxRetry, maxIdleConnections);
        Assert.assertEquals(url, hiveConfigParam.getUrl());
        Assert.assertEquals(user, hiveConfigParam.getUser());
        Assert.assertEquals(password, hiveConfigParam.getPassword());
        Assert.assertEquals(poolSize, hiveConfigParam.getConnectionPoolSize());
        Assert.assertEquals(retryGap, hiveConfigParam.getRetryGapInMillis());
        Assert.assertEquals(maxRetry, hiveConfigParam.getMaxRetries());
    }

}

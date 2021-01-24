package com.flipkart.manager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.exception.TestBedException;
import com.flipkart.utils.HttpURLConnectionUtil;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ha.HealthCheckFailedException;

import java.util.LinkedHashMap;


@Slf4j
public class DSPHealthCheck extends HealthCheckManager {

    @Inject
    public DSPHealthCheck(HttpRequestClient httpRequestClient, HttpURLConnectionUtil httpURLConnectionUtil) {
        super(httpRequestClient, httpURLConnectionUtil);
    }

    @Override
    public Object service(Object serviceUrl) throws TestBedException, HealthCheckFailedException {
        TypeReference<Object> typeReference = new TypeReference<Object>() {};
        Object result = getStatus(serviceUrl.toString(), typeReference);
        log.info("Checking the state of Service");
        if(!(result!=null && ((LinkedHashMap) result).get("hibernate")!=null &&
                ((((LinkedHashMap)((LinkedHashMap) result).get("hibernate"))).get("healthy").equals(true)) )) {
            throw new TestBedException("Service is not in healthy state");
        }
        log.info("Service is up and running");
        return result;
    }
}

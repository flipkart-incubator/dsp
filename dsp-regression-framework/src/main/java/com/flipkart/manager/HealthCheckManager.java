package com.flipkart.manager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.utils.HttpURLConnectionUtil;
import lombok.NoArgsConstructor;
import org.apache.hadoop.ha.HealthCheckFailedException;

import java.io.IOException;
import java.net.HttpURLConnection;


@NoArgsConstructor
public abstract class HealthCheckManager {
    private HttpRequestClient httpRequestClient;
    private HttpURLConnectionUtil httpURLConnectionUtil;

     HealthCheckManager(HttpRequestClient httpRequestClient, HttpURLConnectionUtil httpURLConnectionUtil) {
        this.httpRequestClient = httpRequestClient;
        this.httpURLConnectionUtil = httpURLConnectionUtil;
    }

    public abstract Object service(Object input) throws Exception;


     <T> T getStatus(String targetUrl, TypeReference<T> typeReference) throws HealthCheckFailedException {
        try {
            HttpURLConnection httpURLConnection  = httpURLConnectionUtil.getHttpURLConnection(targetUrl, "application/json", "application/json");
            return httpRequestClient.getRequest(httpURLConnection, typeReference);
        } catch (IOException e) {
            throw new HealthCheckFailedException(e.getMessage());
        }
    }
}

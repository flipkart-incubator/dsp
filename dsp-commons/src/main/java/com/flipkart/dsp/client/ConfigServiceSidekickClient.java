package com.flipkart.dsp.client;


import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.exceptions.ConfigServiceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ConfigServiceSidekickClient {
    private final HttpRequestClient httpRequestClient;

    public Map<String, Object> getConfigBucketForSidekick(String endPoint, String bucketName) throws ConfigServiceException {
        String targetUrl = String.format("http://%s:8800/buckets/%s", endPoint, bucketName);
        log.info("targetUrl : " + targetUrl);

        try {
            HttpURLConnection httpURLConnection = getHttpURLConnection(targetUrl);
            LinkedHashMap<String, Object> configMap = httpRequestClient.getRequest(httpURLConnection, new TypeReference<Object>() {});
            return (Map<String, Object>) configMap.get("keys");
        } catch (IOException e) {
            throw new ConfigServiceException("Exception while getting config Bucket from sidekick");
        }
    }

    private HttpURLConnection getHttpURLConnection(String targetUrl) throws IOException {
        URL url = new URL(targetUrl);
        HttpURLConnection request = (HttpURLConnection) url.openConnection();
        request.setRequestProperty("Accept", "application/json");
        request.setRequestProperty("Content-Type", "application/json");
        request.setReadTimeout(3000);
        request.setDoInput(true);
        return request;
    }
}

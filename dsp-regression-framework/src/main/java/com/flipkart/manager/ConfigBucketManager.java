package com.flipkart.manager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.exception.ConfigBucketException;
import com.flipkart.utils.DSPConstants;
import com.flipkart.utils.HttpURLConnectionUtil;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.LinkedHashMap;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ConfigBucketManager {
    private final HttpRequestClient httpRequestClient;
    private final HttpURLConnectionUtil httpURLConnectionUtil;

    public Long getConfigBucketLatestVersion(String bucketName) throws ConfigBucketException {
        String targetUrl = String.format(DSPConstants.CONFIG_BUCKET_LATEST_VERSION_URL_FORMAT, bucketName);
        TypeReference<Object> typeReference = new TypeReference<Object>() {};
        Long configBucketVersion;

        try {
            HttpURLConnection httpURLConnection = httpURLConnectionUtil.getHttpURLConnection(targetUrl, "application/json", "application/json");
            Object result = httpRequestClient.getRequest(httpURLConnection, typeReference);
            if(result == null) {
                throw new ConfigBucketException("Unable to get Config Bucket Latest version for " + bucketName);
            }
            configBucketVersion = Long.parseLong((((LinkedHashMap) result).get("version").toString()));

        } catch (IOException e) {
            throw new ConfigBucketException(e.getMessage());
        }
        return configBucketVersion;
    }

    public void updateConfigBucket(Object payload, String bucketName, String message, Long bucketVersion)  throws ConfigBucketException {
        String targetUrl = String.format(DSPConstants.CONFIG_BUCKET_UPDATE_VERSION_URL_FORMAT, bucketName, message);
        TypeReference<Object> typeReference = new TypeReference<Object>() {};
        try {
            HttpURLConnection httpURLConnection = httpURLConnectionUtil.getHttpURLConnectionForBucketUpdate(targetUrl, bucketVersion);
            httpRequestClient.postRequest(httpURLConnection, payload, typeReference);
        } catch (IOException e) {
            throw new ConfigBucketException(e.getMessage());
        }
    }
}

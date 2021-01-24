package com.flipkart.dsp.utils;

import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import lombok.extern.slf4j.Slf4j;

import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 */
@Slf4j
public class HTTPRequestUtil {

    private static final int RETRY_EXPONENTIAL_FACTOR = 2;

    public static RequestBuilder buildHeader(RequestBuilder requestBuilder, Map<String, String> headerMap) {
        for (Map.Entry<String, String> headerEntry : headerMap.entrySet()) {
            requestBuilder = requestBuilder.addHeader(headerEntry.getKey(), headerEntry.getValue());
        }
        return requestBuilder;
    }

    public static RequestBuilder buildQueryParams(RequestBuilder requestBuilder, Map<String, String> queryParamMap) {
        for (Map.Entry<String, String> queryParamEntry : queryParamMap.entrySet()) {
            requestBuilder = requestBuilder.addQueryParam(queryParamEntry.getKey(), queryParamEntry.getValue());
        }
        return requestBuilder;
    }

    public static RequestBuilder buildHeaderAndQueryParams(RequestBuilder requestBuilder, Map<String, String> headerMap, Map<String, String> queryParamMap) {
        return buildQueryParams(buildHeader(requestBuilder, headerMap), queryParamMap);
    }

    public static RequestBuilder buildHeaderAndBody(RequestBuilder requestBuilder, Map<String, String> headerMap, String body) {
        return buildHeader(requestBuilder, headerMap).setBody(body);
    }

    public static String buildHTTPURL(String host, Integer port) {
        return "http://" + host + (port != 80 ? (":" + port) : "");
    }

    public static String buildHTTPURL(String host, Integer port, String path) {
        return buildHTTPURL(host, port) + path;
    }

    public static Response executeWithRetries(Callable<Response> operation, int maxRetires, int retryGapInMillis) throws Exception {
        int retryCount = 0;
        while (true) {
            try {
                Response response = operation.call();
                if (!(HttpStatusCodeFamily.fromStatusCode(response.getStatusCode()) == HttpStatusCodeFamily.SERVER_ERROR)) {
                    return response;
                }
                log.warn("[executeWithRetries] RetryCount : " + retryCount + "Failed with status code : " + response.getStatusCode() + "  with message : " + response.getResponseBody());
                if (++retryCount >= maxRetires) {
                    throw new Exception("Http Call Failed with status code : " + response.getStatusCode() + "  with message : " + response.getResponseBody());
                }
            } catch (Exception e) {
                if (e instanceof SocketTimeoutException) {
                    if (++retryCount >= maxRetires) {
                        throw e;
                    }
                    log.error("[executeWithRetries] RetryCount : " + retryCount + "Failed with exception : " + e);
                } else {
                    throw e;
                }
            }
            log.info("Waiting for " + retryGapInMillis + " ms ......");
            Thread.sleep(retryGapInMillis);
            retryGapInMillis = retryGapInMillis * RETRY_EXPONENTIAL_FACTOR;
        }
    }
}

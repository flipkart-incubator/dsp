package com.flipkart.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpURLConnectionUtil {

    public HttpURLConnection getHttpURLConnection(String targetUrl,
                                                         String acceptType,
                                                         String contentType) throws IOException {
        URL url = new URL(targetUrl);
        HttpURLConnection request = (HttpURLConnection) url.openConnection();
        request.setRequestProperty("Accept", acceptType);
        request.setRequestProperty("Content-Type", contentType);
        request.setDoOutput(true);
        request.setDoInput(true);
        return request;
    }


    public HttpURLConnection getHttpURLConnectionForBucketUpdate(String targetUrl, Long version) throws IOException {
        URL url = new URL(targetUrl);
        HttpURLConnection request = (HttpURLConnection) url.openConnection();
        request.setRequestMethod("POST");
        request.setRequestProperty("Content-Type","application/json");
        request.setRequestProperty("Authorization","Basic " + System.getenv("DSP_BUCKET_KEY"));
        request.setRequestProperty("X-Config-Bucket-Version", version.toString());
        request.setDoOutput(true);
        request.setDoInput(true);
        return request;
    }
}

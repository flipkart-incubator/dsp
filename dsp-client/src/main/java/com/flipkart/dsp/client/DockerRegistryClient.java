package com.flipkart.dsp.client;

import com.flipkart.dsp.client.exceptions.DockerRegistryClientException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static java.util.Objects.isNull;

/**
 */
@Slf4j
@Singleton
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DockerRegistryClient {
    private final HttpRequestClient httpRequestClient;

    public String getLatestImageDigest(String url) throws DockerRegistryClientException {
        try {
            HttpURLConnection request = getHttpURLConnection(url);
            httpRequestClient.getRequest(request);
            int responseCode = request.getResponseCode();
            if (responseCode != 200) {
                log.error("Failed to retrieve image digest from docker registry, url {} with error code {}", url, responseCode);
                throw new DockerRegistryClientException("Failed to retrieve image digest from docker registry, url" + url + " with error code " + responseCode);
            }
            if (isNull(request.getHeaderField("Docker-Content-Digest"))) {
                log.error("Unable to retrieve latest image digest from headers");
                throw new DockerRegistryClientException("Unable to retrieve latest image digest from headers");
            }
            return request.getHeaderField("Docker-Content-Digest");
        } catch (IOException e) {
            log.error("Failed to retrieve image digest from docker registry for ", url, e);
            throw new DockerRegistryClientException("Failed to retrieve image digest from docker registry", e);
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

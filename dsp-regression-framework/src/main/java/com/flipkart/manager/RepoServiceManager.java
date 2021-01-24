package com.flipkart.manager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.exception.RepoServiceException;
import com.flipkart.utils.DSPConstants;
import com.flipkart.utils.HttpURLConnectionUtil;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.LinkedHashMap;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RepoServiceManager {
    private final HttpRequestClient httpRequestClient;
    private final HttpURLConnectionUtil httpURLConnectionUtil;

    public Long getLatestRepoVersion(String repoName, String environment) throws RepoServiceException {
        repoName = String.format(repoName, environment);
        String targetUrl = String.format(DSPConstants.REPO_LATEST_VERSION_URL_FORMAT, repoName);
        TypeReference<Object> typeReference = new TypeReference<Object>() {
        };
        Long repoLatestVersion;
        try {
            HttpURLConnection httpURLConnection = httpURLConnectionUtil.getHttpURLConnection(targetUrl, "application/json", "application/json");
            Object result = httpRequestClient.getRequest(httpURLConnection, typeReference);
            if (result == null) {
                throw new RepoServiceException("Unable to get Repo Version for Repo " + repoName);
            }
            repoLatestVersion = Long.parseLong((((LinkedHashMap) result).get("version").toString()));
        } catch (IOException e) {
            throw new RepoServiceException(e.getMessage());
        }
        return repoLatestVersion;
    }

    public String getPkgVersionForSpecificRepo(String repoName, String environment, Long repoVersion) throws RepoServiceException {
        repoName = String.format(repoName, environment);
        String targetUrl = String.format(DSPConstants.REPO_PACKAGE_VERSION_URL_FORMAT, repoName, repoVersion);
        TypeReference<Object> typeReference = new TypeReference<Object>() {
        };
        String repoPackageVersion;
        try {
            HttpURLConnection httpURLConnection = httpURLConnectionUtil.getHttpURLConnection(targetUrl, "application/json", "application/json");
            Object result = httpRequestClient.getRequest(httpURLConnection, typeReference);
            if (result == null) {
                throw new RepoServiceException("Unable to get Repo Version for Repo " + repoName);
            }
            repoPackageVersion = ((LinkedHashMap) ((((ArrayList) ((LinkedHashMap) result).get("results"))).get(0))).get("ver").toString();
        } catch (IOException e) {
            throw new RepoServiceException(e.getMessage());
        }
        return repoPackageVersion;
    }
}

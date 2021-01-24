package com.flipkart.dsp.helper;

import com.flipkart.dsp.actors.ExecutionEnvironmentSnapShotActor;
import com.flipkart.dsp.client.DockerRegistryClient;
import com.flipkart.dsp.client.exceptions.DockerRegistryClientException;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.utils.Constants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ImageDetailsHelper {
    private final DockerRegistryClient dockerRegistryClient;
    private final ExecutionEnvironmentSnapShotActor executionEnvironmentSnapshotActor;

    public String getLatestImageDigest(ExecutionEnvironmentSummary executionEnvironmentSummary) throws DockerRegistryClientException {
        String imagePath = executionEnvironmentSummary.getImagePath();
        String dockerRegistryHost = imagePath.substring(0, imagePath.indexOf("/"));
        String dockerImageName = imagePath.substring(imagePath.indexOf("/") + 1, imagePath.indexOf(":"));
        String dockerImageVersion = imagePath.substring(imagePath.indexOf(":") + 1);
        String url = Constants.http + dockerRegistryHost + "/v2/" + dockerImageName + "/manifests/" + dockerImageVersion;
        return dockerRegistryClient.getLatestImageDigest(url);
    }

    public String getLatestImageDigestInDb(ExecutionEnvironmentSummary executionEnvironmentSummary) {
        return executionEnvironmentSnapshotActor.getLatestExecutionEnvironmentSnapShot(executionEnvironmentSummary.getId()).getLatestImageDigest();
    }

    public boolean isImageUpdated(String latestImageDigest, String latestImageDigestInDb) {
        return !latestImageDigest.equalsIgnoreCase(latestImageDigestInDb);
    }
}

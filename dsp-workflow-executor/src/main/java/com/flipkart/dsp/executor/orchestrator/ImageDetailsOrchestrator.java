package com.flipkart.dsp.executor.orchestrator;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.DockerRegistryClient;
import com.flipkart.dsp.client.exceptions.DockerRegistryClientException;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.executor.exception.ExtractImageSpecificLibraryException;
import com.flipkart.dsp.models.ExecutionEnvironmentDetails;
import com.flipkart.dsp.entities.misc.ImageDetailPayload;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.executor.extractor.ExtractImageSpecificLibrary;
import com.flipkart.dsp.executor.runner.ScriptRunner;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.utils.Constants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ImageDetailsOrchestrator {
    private final ObjectMapper objectMapper;
    private final ScriptRunner scriptRunner;
    private final DSPServiceClient dspServiceClient;
    private final DockerRegistryClient dockerRegistryClient;
    private final ExtractImageSpecificLibrary extractImageSpecificLibrary;

    @Timed
    @Metered
    public void run(LocalScript script, ImageDetailPayload imageDetailPayload) throws ScriptExecutionEngineException, DockerRegistryClientException, IOException, ExtractImageSpecificLibraryException {
        Set<ScriptVariable> outputVariables = scriptRunner.run(script);
        long latestVersion = getLatestImageVersionForImage(imageDetailPayload);
        String latestImageDigest = getLatestImageDigest(imageDetailPayload.getExecutionEnvironmentSummary());
        String librariesString = outputVariables.iterator().next().getValue().toString();
        ExecutionEnvironmentDetails executionEnvironmentDetails = objectMapper.readValue(librariesString, ExecutionEnvironmentDetails.class);
        Map<String, ExecutionEnvironmentDetails.NativeLibraryDetails> imageSpecificLibraries = extractImageSpecificLibrary.getImageSpecificLibrary(executionEnvironmentDetails.getNativeLibrary());
        ExecutionEnvironmentSnapshot executionEnvironmentSnapshot = ExecutionEnvironmentSnapshot.builder()
                .executionEnvironmentId(imageDetailPayload.getExecutionEnvironmentSummary().getId())
                .librarySet(objectMapper.writeValueAsString(executionEnvironmentDetails.getLibrary()))
                .nativeLibrarySet(objectMapper.writeValueAsString(imageSpecificLibraries))
                .os(executionEnvironmentDetails.getOs())
                .osVersion(executionEnvironmentDetails.getOsVersion())
                .imageLanguageVersion(executionEnvironmentDetails.getImageLanguageVersion())
                .version(latestVersion + 1)
                .latestImageDigest(latestImageDigest)
                .build();
        dspServiceClient.saveExecutionEnvironmentSnapshotRequest(executionEnvironmentSnapshot);
    }


    private long getLatestImageVersionForImage(ImageDetailPayload imageDetailPayload) {
        List<ExecutionEnvironmentSnapshot> executionEnvironmentSnapshots = imageDetailPayload.getExecutionEnvironmentSnapshots();
        if (executionEnvironmentSnapshots == null || executionEnvironmentSnapshots.size() == 0)
            return 0;
        executionEnvironmentSnapshots.sort(Comparator.comparing(ExecutionEnvironmentSnapshot::getVersion).reversed());
        return executionEnvironmentSnapshots.get(0).getVersion();
    }

    private String getLatestImageDigest(ExecutionEnvironmentSummary executionEnvironmentSummary) throws DockerRegistryClientException {
        String imagePath = executionEnvironmentSummary.getImagePath();
        String dockerRegistryHost = imagePath.substring(0, imagePath.indexOf("/"));
        String dockerImageName = imagePath.substring(imagePath.indexOf("/") + 1, imagePath.indexOf(":"));
        String dockerImageVersion = imagePath.substring(imagePath.indexOf(":") + 1);
        String url = Constants.http + dockerRegistryHost + "/v2/" + dockerImageName + "/manifests/" + dockerImageVersion;
        return dockerRegistryClient.getLatestImageDigest(url);
    }
}

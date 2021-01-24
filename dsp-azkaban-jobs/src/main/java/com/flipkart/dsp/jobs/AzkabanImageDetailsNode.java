package com.flipkart.dsp.jobs;

import com.flipkart.dsp.actors.ExecutionEnvironmentActor;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DockerRegistryClientException;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.helper.ImageDetailsHelper;
import com.flipkart.dsp.mesos.ImageDetailsMesosExecutionDriver;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.NodeMetaData;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class AzkabanImageDetailsNode implements SessionfulApplication {

    private final DSPServiceClient dspServiceClient;
    private final ImageDetailsHelper imageDetailsHelper;
    private final ExecutionEnvironmentActor executionEnvironmentActor;
    private final ImageDetailsMesosExecutionDriver imageDetailsMesosExecutionDriver;

    @Inject
    public AzkabanImageDetailsNode(DSPServiceClient dspServiceClient,
                                   ImageDetailsHelper imageDetailsHelper,
                                   ExecutionEnvironmentActor executionEnvironmentActor,
                                   ImageDetailsMesosExecutionDriver imageDetailsMesosExecutionDriver) {
        this.dspServiceClient = dspServiceClient;
        this.imageDetailsHelper = imageDetailsHelper;
        this.executionEnvironmentActor = executionEnvironmentActor;
        this.imageDetailsMesosExecutionDriver = imageDetailsMesosExecutionDriver;
    }

    @Override
    public String getName() {
        return Constants.IMAGE_DETAILS_NODE;
    }

    @Override
    public NodeMetaData execute(String[] args) throws AzkabanException {
        try {
            List<ExecutionEnvironmentSummary> executionEnvironmentSummaryList = executionEnvironmentActor.getExecutionEnvironmentsSummary();
            List<ExecutionEnvironmentSummary> updatedEnvironments = new ArrayList<>();
            for (ExecutionEnvironmentSummary environment : executionEnvironmentSummaryList) {
                String latestImageDigest = imageDetailsHelper.getLatestImageDigest(environment);
                String latestImageDigestInDb = imageDetailsHelper.getLatestImageDigestInDb(environment);
                if (imageDetailsHelper.isImageUpdated(latestImageDigest, latestImageDigestInDb))
                    updatedEnvironments.add(environment);
            }
            imageDetailsMesosExecutionDriver.execute(updatedEnvironments);
            return null;
        } catch (DockerRegistryClientException e) {
            throw new AzkabanException("Azkaban node failed because of following reason:", e);
        } finally {
            dspServiceClient.close();
        }
    }
}

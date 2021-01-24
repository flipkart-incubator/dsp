package com.flipkart.dsp.jobs;

import com.flipkart.dsp.actors.ExternalHealthCheckAuditActor;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.enums.ExternalHealthCheckStatus;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.exceptions.HealthCheckException;
import com.flipkart.dsp.health_check.*;
import com.flipkart.dsp.models.ExternalClient;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.NodeMetaData;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * +
 */
@Slf4j
public class AzkabanExternalClientHealthCheckNode implements SessionfulApplication {

    private DSPServiceClient dspServiceClient;
    private GithubHealthCheck githubHealthCheck;
    private DockerRegistryHealthCheck dockerRegistryHealthCheck;
    private ExternalHealthCheckAuditActor externalHealthCheckAuditActor;

    @Inject
    public AzkabanExternalClientHealthCheckNode(
            DSPServiceClient dspServiceClient,
            GithubHealthCheck githubHealthCheck,
            DockerRegistryHealthCheck dockerRegistryHealthCheck,
            ExternalHealthCheckAuditActor externalHealthCheckAuditActor) {
        this.dspServiceClient = dspServiceClient;
        this.githubHealthCheck = githubHealthCheck;
        this.dockerRegistryHealthCheck = dockerRegistryHealthCheck;
        this.externalHealthCheckAuditActor = externalHealthCheckAuditActor;
    }

    @Override
    public String getName() {
        return Constants.HEALTH_CHECK_NODE;
    }

    @Override
    public NodeMetaData execute(String[] args) throws AzkabanException {
        Map<ExternalClient, HealthCheck> externalClientEnumHealthCheckMap = populateMap();
        List<String> errorMessages = new ArrayList<>();
        for (ExternalClient externalClient : ExternalClient.values()) {
            try {
                externalClientEnumHealthCheckMap.get(externalClient).check();
                externalHealthCheckAuditActor.createExternalHealthCheckAudit(ExternalHealthCheckStatus.SUCCESSFUL, externalClient);
            } catch (HealthCheckException ex) {
                errorMessages.add(ex.getMessage());
                externalHealthCheckAuditActor.createExternalHealthCheckAudit(ExternalHealthCheckStatus.FAILED, externalClient);
            }
        }

        dspServiceClient.close();

        if (errorMessages.size() != 0) {
            List<String> failedClients = errorMessages.stream().map(error -> error.replace("Health Check failed for ", "")).collect(Collectors.toList());
            String errorMessage = "Health Check Failed For following clients " + failedClients.toString();
            throw new AzkabanException(errorMessage);
        }

        return null;
    }

    private Map<ExternalClient, HealthCheck> populateMap() {
        Map<ExternalClient, HealthCheck> externalClientEnumHealthCheckMap = new HashMap<>();
        externalClientEnumHealthCheckMap.put(ExternalClient.GITHUB, githubHealthCheck);
        externalClientEnumHealthCheckMap.put(ExternalClient.DOCKER_REGISTRY, dockerRegistryHealthCheck);
        return externalClientEnumHealthCheckMap;
    }
}

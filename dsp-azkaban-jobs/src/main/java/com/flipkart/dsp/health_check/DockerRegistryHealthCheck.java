package com.flipkart.dsp.health_check;

import com.flipkart.dsp.actors.ExecutionEnvironmentActor;
import com.flipkart.dsp.exceptions.HealthCheckException;
import com.flipkart.dsp.helper.ImageDetailsHelper;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.models.ExternalClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.HEALTH_CHECK_ERROR;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DockerRegistryHealthCheck implements HealthCheck {
    private final ImageDetailsHelper imageDetailsHelper;
    private final ExecutionEnvironmentActor executionEnvironmentActor;

    public void check() throws HealthCheckException {
        List<ExecutionEnvironmentSummary> executionEnvironmentSummaryList = executionEnvironmentActor.getExecutionEnvironmentsSummary();
        try {
            imageDetailsHelper.getLatestImageDigest(executionEnvironmentSummaryList.get(0));
        } catch (Exception e) {
            throw new HealthCheckException(String.format(HEALTH_CHECK_ERROR, ExternalClient.DOCKER_REGISTRY.toString()), e);
        }
    }
}

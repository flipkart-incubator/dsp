package com.flipkart.dsp.health_check;

import com.flipkart.dsp.actors.ExecutionEnvironmentActor;
import com.flipkart.dsp.client.exceptions.DockerRegistryClientException;
import com.flipkart.dsp.exceptions.HealthCheckException;
import com.flipkart.dsp.helper.ImageDetailsHelper;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.models.ExternalClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.HEALTH_CHECK_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class DockerRegistryHealthCheckTest {

    @Mock private ImageDetailsHelper imageDetailsHelper;
    @Mock private ExecutionEnvironmentActor executionEnvironmentActor;
    @Mock private ExecutionEnvironmentSummary executionEnvironmentSummary;

    private DockerRegistryHealthCheck dockerRegistryHealthCheck;
    private List<ExecutionEnvironmentSummary> executionEnvironmentList = new ArrayList<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.dockerRegistryHealthCheck = new DockerRegistryHealthCheck(imageDetailsHelper, executionEnvironmentActor);
        executionEnvironmentList.add(executionEnvironmentSummary);
    }

    @Test
    public void testCheckSuccess() throws Exception {
        when(executionEnvironmentActor.getExecutionEnvironmentsSummary()).thenReturn(executionEnvironmentList);
        when(imageDetailsHelper.getLatestImageDigest(executionEnvironmentSummary)).thenReturn("imageDigest");

        dockerRegistryHealthCheck.check();
        verify(executionEnvironmentActor, times(1)).getExecutionEnvironmentsSummary();
        verify(imageDetailsHelper, times(1)).getLatestImageDigest(executionEnvironmentSummary);
    }

    @Test
    public void testCheckFailure() throws Exception {
        boolean isException = false;
        when(executionEnvironmentActor.getExecutionEnvironmentsSummary()).thenReturn(executionEnvironmentList);
        when(imageDetailsHelper.getLatestImageDigest(executionEnvironmentSummary)).thenThrow(new DockerRegistryClientException("Exception"));

        try {
            dockerRegistryHealthCheck.check();
        } catch (HealthCheckException e) {
            isException = true;
            assertEquals(e.getMessage(), String.format(HEALTH_CHECK_ERROR, ExternalClient.DOCKER_REGISTRY));
        }

        assertTrue(isException);
        verify(executionEnvironmentActor, times(1)).getExecutionEnvironmentsSummary();
        verify(imageDetailsHelper, times(1)).getLatestImageDigest(executionEnvironmentSummary);
    }
}

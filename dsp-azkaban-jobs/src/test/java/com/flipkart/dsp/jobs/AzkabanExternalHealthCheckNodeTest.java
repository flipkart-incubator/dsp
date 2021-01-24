package com.flipkart.dsp.jobs;

import com.flipkart.dsp.actors.ExternalHealthCheckAuditActor;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.enums.ExternalHealthCheckStatus;
import com.flipkart.dsp.exceptions.HealthCheckException;
import com.flipkart.dsp.health_check.DockerRegistryHealthCheck;
import com.flipkart.dsp.health_check.GithubHealthCheck;
import com.flipkart.dsp.models.ExternalClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.HEALTH_CHECK_ERROR;
import static com.flipkart.dsp.utils.Constants.HEALTH_CHECK_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class AzkabanExternalHealthCheckNodeTest {

    @Mock
    private DSPServiceClient dspServiceClient;
    @Mock
    private GithubHealthCheck githubHealthCheck;
    @Mock
    private DockerRegistryHealthCheck dockerRegistryHealthCheck;
    @Mock
    private ExternalHealthCheckAuditActor externalHealthCheckAuditActor;
    private AzkabanExternalClientHealthCheckNode azkabanExternalClientHealthCheckNode;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.azkabanExternalClientHealthCheckNode = spy(new AzkabanExternalClientHealthCheckNode(dspServiceClient, githubHealthCheck,
                dockerRegistryHealthCheck, externalHealthCheckAuditActor));

        doNothing().when(dspServiceClient).close();
    }

    @Test
    public void testExecute() throws Exception {
        checkFunctionMocks();
        createExternalHealthCheckAuditFunctionMocks();
        azkabanExternalClientHealthCheckNode.execute(null);

        boolean isException = false;
        doThrow(new HealthCheckException(String.format(HEALTH_CHECK_ERROR, ExternalClient.DOCKER_REGISTRY.toString()))).when(dockerRegistryHealthCheck).check();
        doNothing().when(externalHealthCheckAuditActor).createExternalHealthCheckAudit(ExternalHealthCheckStatus.FAILED, ExternalClient.CONFIG_SVC);
        doNothing().when(externalHealthCheckAuditActor).createExternalHealthCheckAudit(ExternalHealthCheckStatus.FAILED, ExternalClient.DOCKER_REGISTRY);

        try {
            azkabanExternalClientHealthCheckNode.execute(null);
        } catch (Exception e) {
            isException = true;
            List<String> clients = new ArrayList<>();
            clients.add(ExternalClient.CONFIG_SVC.toString());
            clients.add(ExternalClient.DOCKER_REGISTRY.toString());
            assertEquals(e.getMessage(), "Health Check Failed For following clients " + clients.toString());
        }

        assertTrue(isException);
        verifyCheckFunctionMocks();
        verifyCreateExternalHealthCheckAuditFunctionMocks();
    }

    private void checkFunctionMocks() throws Exception {
        doNothing().when(githubHealthCheck).check();
        doNothing().when(dockerRegistryHealthCheck).check();
    }

    private void createExternalHealthCheckAuditFunctionMocks() {
        doNothing().when(externalHealthCheckAuditActor).createExternalHealthCheckAudit(ExternalHealthCheckStatus.SUCCESSFUL, ExternalClient.GITHUB);
        doNothing().when(externalHealthCheckAuditActor).createExternalHealthCheckAudit(ExternalHealthCheckStatus.SUCCESSFUL, ExternalClient.CONFIG_SVC);
        doNothing().when(externalHealthCheckAuditActor).createExternalHealthCheckAudit(ExternalHealthCheckStatus.SUCCESSFUL, ExternalClient.DOCKER_REGISTRY);
    }

    private void verifyCheckFunctionMocks() throws Exception {
        verify(githubHealthCheck, times(2)).check();
        verify(dockerRegistryHealthCheck, times(2)).check();
    }

    private void verifyCreateExternalHealthCheckAuditFunctionMocks() {
        verify(externalHealthCheckAuditActor, times(2)).createExternalHealthCheckAudit(ExternalHealthCheckStatus.SUCCESSFUL, ExternalClient.GITHUB);
        verify(externalHealthCheckAuditActor, times(1)).createExternalHealthCheckAudit(ExternalHealthCheckStatus.SUCCESSFUL, ExternalClient.CONFIG_SVC);
        verify(externalHealthCheckAuditActor, times(1)).createExternalHealthCheckAudit(ExternalHealthCheckStatus.SUCCESSFUL, ExternalClient.DOCKER_REGISTRY);
        verify(externalHealthCheckAuditActor, times(1)).createExternalHealthCheckAudit(ExternalHealthCheckStatus.FAILED, ExternalClient.CONFIG_SVC);
        verify(externalHealthCheckAuditActor, times(1)).createExternalHealthCheckAudit(ExternalHealthCheckStatus.FAILED, ExternalClient.DOCKER_REGISTRY);
    }

    @Test
    public void testGetName() {
        assertEquals(azkabanExternalClientHealthCheckNode.getName(), HEALTH_CHECK_NODE);
    }
}


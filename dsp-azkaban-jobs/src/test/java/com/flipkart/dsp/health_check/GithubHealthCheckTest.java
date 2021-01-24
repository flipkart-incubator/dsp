package com.flipkart.dsp.health_check;

import com.flipkart.dsp.config.GithubConfig;
import com.flipkart.dsp.exceptions.HealthCheckException;
import com.flipkart.dsp.models.ExternalClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static com.flipkart.dsp.utils.Constants.HEALTH_CHECK_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({GitHub.class, GithubConfig.class})
public class GithubHealthCheckTest {
    @Mock private GitHub gitHub;
    @Mock private GHRepository ghRepository;
    @Mock private GithubConfig githubConfig;

    private String repoName = "dsp-sandbox-trial";
    private String dspRepo = "Flipkart/" + repoName;
    private GithubHealthCheck githubHealthCheck;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(GitHub.class);
        this.githubHealthCheck = spy(new GithubHealthCheck());

        PowerMockito.whenNew(GithubConfig.class).withAnyArguments().thenReturn(githubConfig);
        PowerMockito.when(GitHub.connectToEnterpriseWithOAuth(anyString(), anyString(), anyString())).thenReturn(gitHub);
    }

    @Test
    public void testCheckSuccess() throws Exception {
        when(gitHub.getRepository(dspRepo)).thenReturn(ghRepository);
        when(ghRepository.getName()).thenReturn(repoName);
        githubHealthCheck.check();

        verify(gitHub, times(1)).getRepository(dspRepo);
        verify(ghRepository, times(1)).getName();
    }

    @Test
    public void testCheckFailureCase1() throws Exception {
        boolean isException = false;
        when(gitHub.getRepository(dspRepo)).thenReturn(ghRepository);
        when(ghRepository.getName()).thenReturn(dspRepo + "_test");

        try {
            githubHealthCheck.check();
        } catch (HealthCheckException e) {
            isException = true;
            assertEquals(e.getMessage(), String.format(HEALTH_CHECK_ERROR, ExternalClient.GITHUB));
        }

        assertTrue(isException);
        verify(gitHub, times(1)).getRepository(dspRepo);
        verify(ghRepository, times(1)).getName();
    }

    @Test
    public void testCheckFailureCase2() throws Exception {
        boolean isException = false;
        when(gitHub.getRepository(dspRepo)).thenThrow(new IOException());

        try {
            githubHealthCheck.check();
        } catch (HealthCheckException e) {
            isException = true;
            assertEquals(e.getMessage(), String.format(HEALTH_CHECK_ERROR, ExternalClient.GITHUB));
        }

        assertTrue(isException);
        verify(gitHub, times(1)).getRepository(dspRepo);
    }
}

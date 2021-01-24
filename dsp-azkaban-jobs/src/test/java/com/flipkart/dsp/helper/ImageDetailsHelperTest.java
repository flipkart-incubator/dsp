package com.flipkart.dsp.helper;

import com.flipkart.dsp.actors.ExecutionEnvironmentSnapShotActor;
import com.flipkart.dsp.client.DockerRegistryClient;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class ImageDetailsHelperTest {
    @Mock private DockerRegistryClient dockerRegistryClient;
    @Mock private ExecutionEnvironmentSummary executionEnvironmentSummary;
    @Mock private ExecutionEnvironmentSnapshot executionEnvironmentSnapshot;
    @Mock private ExecutionEnvironmentSnapShotActor executionEnvironmentSnapShotActor;

    private String imageDigest = "imageDigest";
    private ImageDetailsHelper imageDetailsHelper;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.imageDetailsHelper = spy(new ImageDetailsHelper(dockerRegistryClient, executionEnvironmentSnapShotActor));
    }

    @Test
    public void testGetLatestImageDigestTest() throws Exception {
        when(executionEnvironmentSummary.getImagePath()).thenReturn("0.0.0.0/jessie-r:3.2.5");
        when(dockerRegistryClient.getLatestImageDigest(anyString())).thenReturn(imageDigest);

        String expected = imageDetailsHelper.getLatestImageDigest(executionEnvironmentSummary);
        assertEquals(expected, imageDigest);
        verify(executionEnvironmentSummary, times(1)).getImagePath();
        verify(dockerRegistryClient, times(1)).getLatestImageDigest(anyString());
    }

    @Test
    public void testGetLatestEnvironmentSnapshot() {
        when(executionEnvironmentSummary.getId()).thenReturn(1L);
        when(executionEnvironmentSnapShotActor.getLatestExecutionEnvironmentSnapShot(1L)).thenReturn(executionEnvironmentSnapshot);
        when(executionEnvironmentSnapshot.getLatestImageDigest()).thenReturn(imageDigest);

        String expected =  imageDetailsHelper.getLatestImageDigestInDb(executionEnvironmentSummary);
        assertNotNull(expected);
        assertEquals(expected, imageDigest);
        verify(executionEnvironmentSummary).getId();
        verify(executionEnvironmentSnapShotActor).getLatestExecutionEnvironmentSnapShot(1L);
        verify(executionEnvironmentSnapshot).getLatestImageDigest();
    }

    @Test
    public void testIsImageUpdated() {
        assertFalse(imageDetailsHelper.isImageUpdated(imageDigest, imageDigest));
        assertTrue(imageDetailsHelper.isImageUpdated(imageDigest, "latestImageDigest"));
    }
}

package com.flipkart.dsp.client;

import com.flipkart.dsp.client.exceptions.DockerRegistryClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({URL.class, DockerRegistryClient.class})
public class DockerRegistryClientTest {

    private URL url;
    @Mock private HttpURLConnection request;
    @Mock private HttpRequestClient httpRequestClient;
    private DockerRegistryClient dockerRegistryClient;
    private String urlPath = "http://0.0.0.0/v2/jessie-py/manifests/2.7.9-torch-1.0.0";

    @Before
    public void setUp() throws Exception {
        url = mock(URL.class);
        MockitoAnnotations.initMocks(this);
        this.dockerRegistryClient = spy(new DockerRegistryClient(httpRequestClient));

        whenNew(URL.class).withAnyArguments().thenReturn(url);
        when(url.openConnection()).thenReturn(request);
    }

    @Test
    public void testLatestImageDigestFailureCase1() throws Exception {
        boolean isException = false;
        when(httpRequestClient.getRequest(request)).thenThrow(new IOException(""));

        try {
            dockerRegistryClient.getLatestImageDigest(urlPath);
        } catch (DockerRegistryClientException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Failed to retrieve image digest from docker registry"));
        }

        assertTrue(isException);

        verify(url, times(1)).openConnection();
        verify(httpRequestClient, times(1)).getRequest(request);
    }

    @Test
    public void testLatestImageDigestFailureCase2() throws Exception {
        boolean isException = false;
        when(httpRequestClient.getRequest(request)).thenReturn("");
        when(request.getResponseCode()).thenReturn(400);

        try {
            dockerRegistryClient.getLatestImageDigest(urlPath);
        } catch (DockerRegistryClientException e) {
            isException = true;
            assertEquals(e.getMessage(),"Failed to retrieve image digest from docker registry, url" + urlPath + " with error code " + request.getResponseCode());
        }

        assertTrue(isException);
        verify(url, times(1)).openConnection();
        verify(httpRequestClient, times(1)).getRequest(request);
        verify(request, times(2)).getResponseCode();
    }

    @Test
    public void testLatestImageDigestFailureCase3() throws Exception {
        boolean isException = false;
        when(httpRequestClient.getRequest(request)).thenReturn("");
        when(request.getResponseCode()).thenReturn(200);
        when(request.getHeaderField("Docker-Content-Digest")).thenReturn(null);
        try {
            dockerRegistryClient.getLatestImageDigest(urlPath);
        } catch (DockerRegistryClientException e) {
            isException = true;
            assertEquals(e.getMessage(),"Unable to retrieve latest image digest from headers");
        }

        assertTrue(isException);
        verify(url, times(1)).openConnection();
        verify(httpRequestClient, times(1)).getRequest(request);
        verify(request, times(1)).getResponseCode();
        verify(request, times(1)).getHeaderField("Docker-Content-Digest");
    }

    @Test
    public void testLatestImageDigestSuccess() throws Exception {
        when(httpRequestClient.getRequest(request)).thenReturn("");
        when(request.getResponseCode()).thenReturn(200);
        when(request.getHeaderField("Docker-Content-Digest")).thenReturn("latestImageDigest");

        String actual = dockerRegistryClient.getLatestImageDigest(urlPath);
        assertNotNull(actual);
        assertEquals(actual, "latestImageDigest");
        verify(url, times(1)).openConnection();
        verify(httpRequestClient, times(1)).getRequest(request);
        verify(request, times(1)).getResponseCode();
        verify(request, times(2)).getHeaderField("Docker-Content-Digest");
    }
}

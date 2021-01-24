package com.flipkart.dsp.client;

import com.flipkart.dsp.exceptions.ConfigServiceException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class ConfigServiceSidekickClientTest {
    @Mock private HttpRequestClient httpRequestClient;
    private ConfigServiceSidekickClient configServiceSidekickClient;

    private final String endPoint = "localhost";
    private final String bucketName = "dsp-stage";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.configServiceSidekickClient = spy(new ConfigServiceSidekickClient(httpRequestClient));
    }

    @Test
    public void testGetConfigBucketForSidekickSuccess() throws Exception {
        LinkedHashMap<String, Object> configMap = new LinkedHashMap<>();
        LinkedHashMap<String, Object> keys = new LinkedHashMap<>();
        keys.put("key1", "value1");
        configMap.put("keys", keys);
        when(httpRequestClient.getRequest(any(), any())).thenReturn(configMap);

        Map<String, Object> expected = configServiceSidekickClient.getConfigBucketForSidekick(endPoint, bucketName);
        Map<String, Object> expectedKeys = (Map<String, Object>) configMap.get("keys");
        assertNotNull(expected);
        assertNotNull(expectedKeys);
        assertEquals(expected.size(), 1);
        assertEquals(expected.get("key1"), "value1");
        verify(httpRequestClient).getRequest(any(),  any());
    }

    @Test
    public void testGetConfigBucketForSidekickFailure() throws Exception {
        boolean isException = false;
        when(httpRequestClient.getRequest(any(), any())).thenThrow(new IOException(""));

        try {
            configServiceSidekickClient.getConfigBucketForSidekick(endPoint, bucketName);
        } catch (ConfigServiceException e) {
            isException = true;
            assertEquals(e.getMessage(), "Exception while getting config Bucket from sidekick");
        }

        assertTrue(isException);
        verify(httpRequestClient).getRequest(any(), any());
    }
}

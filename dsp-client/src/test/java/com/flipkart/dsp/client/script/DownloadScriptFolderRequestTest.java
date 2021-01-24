package com.flipkart.dsp.client.script;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JsonUtils.class})
public class DownloadScriptFolderRequestTest {

    @Mock private DSPServiceClient serviceClient;
    private Long scriptId = 1L;
    private DownloadScriptFolderRequest downloadScriptFolderRequest;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        MockitoAnnotations.initMocks(this);;
        downloadScriptFolderRequest = spy(new DownloadScriptFolderRequest(scriptId, serviceClient));
    }

    @Test
    public void testGetMethod() {
        assertEquals(downloadScriptFolderRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(downloadScriptFolderRequest.getPath(),"/v1/scripts/" + scriptId + "/download" );
    }

    @Test
    public void testGetReturnType() {
        assertEquals(downloadScriptFolderRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(InputStream.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = downloadScriptFolderRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

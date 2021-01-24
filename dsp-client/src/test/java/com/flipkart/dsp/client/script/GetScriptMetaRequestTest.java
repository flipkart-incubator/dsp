package com.flipkart.dsp.client.script;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.script.ScriptMeta;
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
public class GetScriptMetaRequestTest {

    @Mock private DSPServiceClient serviceClient;
    private Long scriptId = 1L;
    private GetScriptMetaRequest getScriptMetaRequest;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        MockitoAnnotations.initMocks(this);;
        getScriptMetaRequest = spy(new GetScriptMetaRequest(serviceClient, scriptId));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getScriptMetaRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getScriptMetaRequest.getPath(),"/v1/scripts/" + scriptId);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getScriptMetaRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(ScriptMeta.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getScriptMetaRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

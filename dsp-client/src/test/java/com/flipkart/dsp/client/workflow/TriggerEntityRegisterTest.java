package com.flipkart.dsp.client.workflow;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JsonUtils.class})
public class TriggerEntityRegisterTest {

    @Mock private DSPServiceClient serviceClient;
    private long requestId = 1L;
    private String serializedTableList = "list";
    private TriggerEntityRegister triggerEntityRegister;


    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        MockitoAnnotations.initMocks(this);;
        triggerEntityRegister = spy(new TriggerEntityRegister(serviceClient, requestId, serializedTableList));
    }

    @Test
    public void testGetMethod() {
        assertEquals(triggerEntityRegister.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(triggerEntityRegister.getPath(),"/v1/external_compute/register_entity");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(triggerEntityRegister.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = triggerEntityRegister.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

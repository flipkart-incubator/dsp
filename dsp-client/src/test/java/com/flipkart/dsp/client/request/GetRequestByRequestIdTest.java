package com.flipkart.dsp.client.request;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.request.Request;
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
public class GetRequestByRequestIdTest {

    @Mock private DSPServiceClient serviceClient;
    private Long requestId = 1L;
    private GetRequestByRequestId getRequestByRequestId;


    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        MockitoAnnotations.initMocks(this);;
        getRequestByRequestId = spy(new GetRequestByRequestId(serviceClient, requestId));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getRequestByRequestId.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getRequestByRequestId.getPath(),"/v2/requests/" + requestId);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getRequestByRequestId.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Request.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getRequestByRequestId.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

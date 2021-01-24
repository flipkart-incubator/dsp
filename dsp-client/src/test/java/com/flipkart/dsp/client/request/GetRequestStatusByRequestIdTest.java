package com.flipkart.dsp.client.request;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.RequestStatus;
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
public class GetRequestStatusByRequestIdTest {

    @Mock private DSPServiceClient serviceClient;
    private Long requestId = 1L;
    private GetRequestStatusByRequestId getRequestStatusByRequestId;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        MockitoAnnotations.initMocks(this);;
        getRequestStatusByRequestId = spy(new GetRequestStatusByRequestId(serviceClient, requestId));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getRequestStatusByRequestId.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getRequestStatusByRequestId.getPath(),"/v2/requests/status/" + requestId);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getRequestStatusByRequestId.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(RequestStatus.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getRequestStatusByRequestId.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

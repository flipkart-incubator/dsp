package com.flipkart.dsp.client.notification.callback;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.notification.callback.UpdateEntityBatchRequest;
import com.flipkart.dsp.dto.UpdateEntityDTO;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
public class UpdateEntityBatchRequestTest {

    @Mock
    DSPServiceClient serviceClient;
    private UpdateEntityBatchRequest updateEntityBatchRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        List<UpdateEntityDTO> updateEntityDTOS = new ArrayList<>();
        updateEntityDTOS.add(new UpdateEntityDTO());
        updateEntityBatchRequest = spy(new UpdateEntityBatchRequest(serviceClient, updateEntityDTOS));
    }

    @Test
    public void testGetMethod() {
        assertEquals(updateEntityBatchRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(updateEntityBatchRequest.getPath(),"/v1/callbacks/update_entity_batch");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(updateEntityBatchRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = updateEntityBatchRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

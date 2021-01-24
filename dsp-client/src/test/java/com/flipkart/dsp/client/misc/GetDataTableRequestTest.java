package com.flipkart.dsp.client.misc;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.sg.DataTable;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
public class GetDataTableRequestTest {

    @Mock private DSPServiceClient serviceClient;
    private String tableName = "tableName";
    private GetDataTableRequest getDataTableRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        getDataTableRequest = spy(new GetDataTableRequest(serviceClient, tableName));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getDataTableRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getDataTableRequest.getPath(), "/v1/data_tables/" + tableName);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getDataTableRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataTable.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getDataTableRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

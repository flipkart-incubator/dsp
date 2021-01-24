package com.flipkart.dsp.client.dataframe;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
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
public class GetDataFrameOverrideAuditByIdRequestTest {

    @Mock DSPServiceClient serviceClient;
    private Long id = 1L;
    private GetDataFrameOverrideAuditByIdRequest getDataFrameOverrideAuditByIdRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        getDataFrameOverrideAuditByIdRequest = spy(new GetDataFrameOverrideAuditByIdRequest(serviceClient, id));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getDataFrameOverrideAuditByIdRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getDataFrameOverrideAuditByIdRequest.getPath(),"/v1/dataframe_override_audits/" + id);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getDataFrameOverrideAuditByIdRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameOverrideAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getDataFrameOverrideAuditByIdRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

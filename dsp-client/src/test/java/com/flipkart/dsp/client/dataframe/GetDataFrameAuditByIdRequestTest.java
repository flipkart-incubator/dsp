package com.flipkart.dsp.client.dataframe;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
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
public class GetDataFrameAuditByIdRequestTest {

    @Mock DSPServiceClient serviceClient;
    private Long dataFrameAuditId = 1L;
    private GetDataFrameAuditByIdRequest getDataFrameAuditByIdRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        getDataFrameAuditByIdRequest = spy(new GetDataFrameAuditByIdRequest(serviceClient, dataFrameAuditId));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getDataFrameAuditByIdRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getDataFrameAuditByIdRequest.getPath(),"/v1/dataframe_audits/" + dataFrameAuditId);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getDataFrameAuditByIdRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getDataFrameAuditByIdRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

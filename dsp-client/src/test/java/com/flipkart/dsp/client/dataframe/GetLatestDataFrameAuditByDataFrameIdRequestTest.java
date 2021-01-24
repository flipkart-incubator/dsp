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
public class GetLatestDataFrameAuditByDataFrameIdRequestTest {

    private Long dataFrameId = 1L;
    @Mock DSPServiceClient serviceClient;
    private GetLatestDataFrameAuditByDataFrameIdRequest getLatestDataFrameAuditByDataFrameIdRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        getLatestDataFrameAuditByDataFrameIdRequest = spy(new GetLatestDataFrameAuditByDataFrameIdRequest(serviceClient, dataFrameId));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getLatestDataFrameAuditByDataFrameIdRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getLatestDataFrameAuditByDataFrameIdRequest.getPath(),"/v1/dataframe_audits/dataframe_id/" + dataFrameId);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getLatestDataFrameAuditByDataFrameIdRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getLatestDataFrameAuditByDataFrameIdRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

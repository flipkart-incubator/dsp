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
public class GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequestTest {

    @Mock DSPServiceClient serviceClient;
    private Long requestId = 1L;
    private Long dataFrameId = 1L;
    private GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest = spy(new GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest(serviceClient, requestId, dataFrameId));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest.getPath(),"/v1/dataframe_override_audits/dataframe_id/" + dataFrameId + "?request_id=" + requestId);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameOverrideAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

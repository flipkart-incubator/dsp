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
public class GetDataFrameAuditRequestTest {

    @Mock DSPServiceClient serviceClient;
    private Long dataFrameId = 1L;
    private String partitions = "partition";
    private Long dataFrameOverrideAuditId = 1L;
    private GetDataFrameAuditRequest getDataFrameAuditRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        getDataFrameAuditRequest = spy(new GetDataFrameAuditRequest(serviceClient, dataFrameId, dataFrameOverrideAuditId, partitions));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getDataFrameAuditRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getDataFrameAuditRequest.getPath(),"/v1/dataframe_audits/dataframe_id/" + dataFrameId + "?partitions=" + partitions + "&override_audit_id=" + dataFrameOverrideAuditId);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getDataFrameAuditRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getDataFrameAuditRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

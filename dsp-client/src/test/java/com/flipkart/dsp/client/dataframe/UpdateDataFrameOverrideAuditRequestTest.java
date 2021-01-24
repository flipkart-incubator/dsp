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
public class UpdateDataFrameOverrideAuditRequestTest {

    @Mock DSPServiceClient serviceClient;
    private UpdateDataFrameOverrideAuditRequest updateDataFrameOverrideAuditRequest;
    private DataFrameOverrideAudit dataFrameOverrideAudit = DataFrameOverrideAudit.builder().build();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        updateDataFrameOverrideAuditRequest = spy(new UpdateDataFrameOverrideAuditRequest(serviceClient, dataFrameOverrideAudit));
    }

    @Test
    public void testGetMethod() {
        assertEquals(updateDataFrameOverrideAuditRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(updateDataFrameOverrideAuditRequest.getPath(),"/v1/dataframe_override_audits/update");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(updateDataFrameOverrideAuditRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameOverrideAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = updateDataFrameOverrideAuditRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

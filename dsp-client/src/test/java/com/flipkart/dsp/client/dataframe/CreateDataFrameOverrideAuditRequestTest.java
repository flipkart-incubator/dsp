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
public class CreateDataFrameOverrideAuditRequestTest {

    @Mock private DSPServiceClient serviceClient;
    private CreateDataFrameOverrideAuditRequest createDataFrameOverrideAuditRequest;
    private DataFrameOverrideAudit dataFrameOverrideAudit = DataFrameOverrideAudit.builder().build();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        createDataFrameOverrideAuditRequest = spy(new CreateDataFrameOverrideAuditRequest(serviceClient, dataFrameOverrideAudit));
    }

    @Test
    public void testGetMethod() {
        assertEquals(createDataFrameOverrideAuditRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(createDataFrameOverrideAuditRequest.getPath(),"/v1/dataframe_override_audits");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(createDataFrameOverrideAuditRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameOverrideAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = createDataFrameOverrideAuditRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

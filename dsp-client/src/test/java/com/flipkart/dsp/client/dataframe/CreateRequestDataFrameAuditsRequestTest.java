package com.flipkart.dsp.client.dataframe;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
public class CreateRequestDataFrameAuditsRequestTest {

    @Mock DSPServiceClient serviceClient;
    private Long requestId = 1L;
    private Long workflowId = 1L;
    private Long pipelineStepId = 1L;
    private Set<DataFrameAudit> dataFrameAudits = new HashSet<>();
    private CreateRequestDataFrameAuditsRequest createRequestDataFrameAuditsRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        createRequestDataFrameAuditsRequest = spy(new CreateRequestDataFrameAuditsRequest(serviceClient, requestId, workflowId, pipelineStepId, dataFrameAudits));
        DataFrameAudit dataFrameAudit = DataFrameAudit.builder().build();
        dataFrameAudits.add(dataFrameAudit);
    }

    @Test
    public void testGetMethod() {
        assertEquals(createRequestDataFrameAuditsRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(createRequestDataFrameAuditsRequest.getPath(),"/v1/dataframe_audits/1/1/1/create");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(createRequestDataFrameAuditsRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Map.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = createRequestDataFrameAuditsRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

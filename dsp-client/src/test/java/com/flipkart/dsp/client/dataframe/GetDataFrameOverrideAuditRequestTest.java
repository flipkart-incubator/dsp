package com.flipkart.dsp.client.dataframe;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
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
public class GetDataFrameOverrideAuditRequestTest {

    @Mock DSPServiceClient serviceClient;
    private Long dataFrameId = 1L;
    private String inputDataId = "inputDataId";
    private GetDataFrameOverrideAuditRequest getDataFrameOverrideAuditRequest;
    private DataFrameOverrideType dataFrameOverrideType = DataFrameOverrideType.HIVE;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        getDataFrameOverrideAuditRequest = spy(new GetDataFrameOverrideAuditRequest(serviceClient, dataFrameId, inputDataId, dataFrameOverrideType));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getDataFrameOverrideAuditRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getDataFrameOverrideAuditRequest.getPath(),"/v1/dataframe_override_audits/dataframe_id/" + dataFrameId + "?input_data_id=" + inputDataId + "&override_type=" + dataFrameOverrideType);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getDataFrameOverrideAuditRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameOverrideAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getDataFrameOverrideAuditRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

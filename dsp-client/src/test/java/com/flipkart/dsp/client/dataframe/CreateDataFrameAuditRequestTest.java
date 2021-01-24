package com.flipkart.dsp.client.dataframe;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.utils.JsonUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JsonUtils.class})
public class CreateDataFrameAuditRequestTest {

    @Mock private DSPServiceClient serviceClient;
    private CreateDataFrameAuditRequest createDataFrameAuditRequest;
    private DataFrameAudit dataFrameAudit = DataFrameAudit.builder().build();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        MockitoAnnotations.initMocks(this);;
        createDataFrameAuditRequest = spy(new CreateDataFrameAuditRequest(serviceClient, dataFrameAudit));
    }

    @Test
    public void testGetMethod() {
        assertEquals(createDataFrameAuditRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(createDataFrameAuditRequest.getPath(),"/v1/dataframe_audits");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(createDataFrameAuditRequest.getReturnType(),JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameAudit.class));
    }
//
//    @Test
//    public void testBuildRequest() {
//        RequestBuilder requestBuilder = new RequestBuilder();
//        RequestBuilder actual = createDataFrameAuditRequest.buildRequest(requestBuilder);
//        assertNotNull(actual);
//    }
}

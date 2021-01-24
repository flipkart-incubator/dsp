package com.flipkart.dsp.client.misc;


import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.event_audits.EventAudit;
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
public class CreateEventAuditRequestTest {

    @Mock private DSPServiceClient serviceClient;

    private String tableName = "tableName";
    private CreateEventAuditRequest createEventAuditRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        EventAudit eventAudit = EventAudit.builder().build();
        createEventAuditRequest = spy(new CreateEventAuditRequest(serviceClient, eventAudit));
    }

    @Test
    public void testGetMethod() {
        assertEquals(createEventAuditRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(createEventAuditRequest.getPath(), "/v1/event_audit/create");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(createEventAuditRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = createEventAuditRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

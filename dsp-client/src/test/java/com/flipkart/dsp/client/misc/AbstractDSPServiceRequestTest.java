package com.flipkart.dsp.client.misc;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.sg.DataTable;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class AbstractDSPServiceRequestTest {
    @Mock private DataTable dataTable;
    @Mock private RequestBuilder requestBuilder;
    @Mock private DSPServiceClient dspServiceClient;
    @Mock private Function<String, String> transform;

    private String tableName = "tableName";
    private AbstractDSPServiceRequest abstractDSPServiceRequest;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.abstractDSPServiceRequest = spy(new GetDataTableRequest(dspServiceClient, tableName));
    }

    @Test
    public void testExecuteSync() {
        when(abstractDSPServiceRequest.getMethod()).thenReturn("POST");
        when(abstractDSPServiceRequest.getPath()).thenReturn("/path");
        when(abstractDSPServiceRequest.getReturnType()).thenReturn(JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataTable.class));
        when(abstractDSPServiceRequest.buildRequest(any())).thenReturn(requestBuilder);
        when(dspServiceClient.executesSync(anyString(), anyString(), any(), any())).thenReturn(dataTable);

        Object expected = abstractDSPServiceRequest.executeSync();
        assertEquals(expected, dataTable);
        verify(abstractDSPServiceRequest).getMethod();
        verify(abstractDSPServiceRequest).getPath();
        verify(abstractDSPServiceRequest).getReturnType();
//        verify(abstractDSPServiceRequest).buildRequest(any());
        verify(dspServiceClient).executesSync(anyString(), anyString(), any(), any());
    }

    @Test
    public void testAddFormParamCase1() {
        when(requestBuilder.addQueryParam(anyString(), anyString())).thenReturn(requestBuilder);
        abstractDSPServiceRequest.addFormParam(requestBuilder, "key", null);
        abstractDSPServiceRequest.addFormParam(requestBuilder, "key", "value");
    }

    @Test
    public void testAddFormParamCase2() {
        when(transform.apply(any())).thenReturn("output");
        when(requestBuilder.addQueryParam(anyString(), anyString())).thenReturn(requestBuilder);
        abstractDSPServiceRequest.addFormParam(requestBuilder, "key", null, transform);
        abstractDSPServiceRequest.addFormParam(requestBuilder, "key", "value", transform);
    }

    @Test
    public void testAddQueryParamCase1() {
        when(requestBuilder.addQueryParam(anyString(), anyString())).thenReturn(requestBuilder);
        abstractDSPServiceRequest.addQueryParam(requestBuilder, "key", null);
        abstractDSPServiceRequest.addQueryParam(requestBuilder, "key", "value");
    }

    @Test
    public void testAddQueryParamCase2() {
        when(requestBuilder.addQueryParam(anyString(), anyString())).thenReturn(requestBuilder);
        abstractDSPServiceRequest.addQueryParam(requestBuilder, "key", null, transform);
        abstractDSPServiceRequest.addQueryParam(requestBuilder, "key", "value", transform);
    }


}

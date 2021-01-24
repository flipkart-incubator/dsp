package com.flipkart.dsp.client.misc;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.ExternalCredentials;
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
public class GetExternalCredentialsByClientAliasRequestTest {

    @Mock private DSPServiceClient serviceClient;
    private String clientAlias = "clientAlias";
    private GetExternalCredentialsByClientAliasRequest getExternalCredentialsByClientAliasRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        getExternalCredentialsByClientAliasRequest = spy(new GetExternalCredentialsByClientAliasRequest(serviceClient, clientAlias));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getExternalCredentialsByClientAliasRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getExternalCredentialsByClientAliasRequest.getPath(), "/v2/external_credentials/" + clientAlias);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getExternalCredentialsByClientAliasRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(ExternalCredentials.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getExternalCredentialsByClientAliasRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}

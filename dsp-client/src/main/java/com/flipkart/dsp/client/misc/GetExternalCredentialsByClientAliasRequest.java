package com.flipkart.dsp.client.misc;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 * +
 */
public class GetExternalCredentialsByClientAliasRequest extends AbstractDSPServiceRequest<ExternalCredentials> {
    private String clientAlias;

    public GetExternalCredentialsByClientAliasRequest(DSPServiceClient dspServiceClient, String clientAlias) {
        super(dspServiceClient);
        this.clientAlias = clientAlias;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v2/external_credentials/" + clientAlias;
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(ExternalCredentials.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }
}

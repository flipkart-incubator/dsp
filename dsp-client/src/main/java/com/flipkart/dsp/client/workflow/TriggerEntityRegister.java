package com.flipkart.dsp.client.workflow;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 */
public class TriggerEntityRegister extends AbstractDSPServiceRequest<Void> {
    private long requestId;
    private String serializedTableList;

    public TriggerEntityRegister(DSPServiceClient client, long requestId, String serializedTableList) {
        super(client);
        this.requestId = requestId;
        this.serializedTableList = serializedTableList;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/v1/external_compute/register_entity";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED);
        requestBuilder.addFormParam("requestId", String.valueOf(requestId));
        requestBuilder.addFormParam("tables", String.valueOf(serializedTableList));
        return requestBuilder;
    }
}

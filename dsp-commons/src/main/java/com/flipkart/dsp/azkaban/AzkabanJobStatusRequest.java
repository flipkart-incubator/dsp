package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 */
public class AzkabanJobStatusRequest extends AbstractAzkabanRequest<AzkabanJobStatusResponse> {

    private String sessionId;
    private long execId;

    public AzkabanJobStatusRequest(AzkabanClient client, String sessionId, long execId){
        super(client);
        this.sessionId = sessionId;
        this.execId = execId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/executor";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(AzkabanJobStatusResponse.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.addHeader("Content-type", "application/x-www-form-urlencoded");
        requestBuilder.addHeader("X-Requested-With", "XMLHttpRequest");

        requestBuilder.addQueryParam("ajax", "fetchexecflow");
        requestBuilder.addQueryParam("session.id", this.sessionId);
        requestBuilder.addQueryParam("execid", Long.toString(this.execId));
        return requestBuilder;
    }
}

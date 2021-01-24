package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 */
public class KillAzkabanJob extends AbstractAzkabanRequest<AzkabanJobKillResponse> {

    private String sessionId;
    private Long execId;

    public KillAzkabanJob(AzkabanClient client, String sessionId, long execId){
        super(client);
        this.sessionId = sessionId;
        this.execId = execId;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/executor";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(AzkabanJobKillResponse.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.addHeader("Content-type", "application/x-www-form-urlencoded");
        requestBuilder.addHeader("X-Requested-With", "XMLHttpRequest");

        requestBuilder.addQueryParam("ajax", "cancelFlow");
        requestBuilder.addQueryParam("session.id", this.sessionId);
        requestBuilder.addQueryParam("execid", Long.toString(this.execId));
        return requestBuilder;
    }
}

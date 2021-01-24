package com.flipkart.dsp.client.misc;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

public class CreateEventAuditRequest extends AbstractDSPServiceRequest<Void> {

    private final EventAudit eventAudit;

    public CreateEventAuditRequest(DSPServiceClient client, EventAudit eventAudit) {
        super(client);
        this.eventAudit = eventAudit;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/v1/event_audit/create";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(eventAudit));
        return requestBuilder;
    }
}

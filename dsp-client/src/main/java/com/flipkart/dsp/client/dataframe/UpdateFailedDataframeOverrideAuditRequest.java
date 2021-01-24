package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

public class UpdateFailedDataframeOverrideAuditRequest extends AbstractDSPServiceRequest<Void> {
    private Long requestId;

    public UpdateFailedDataframeOverrideAuditRequest(DSPServiceClient client, Long requestId) {
        super(client);
        this.requestId = requestId;
    }

    @Override
    protected String getMethod() {
        return "PATCH";
    }

    @Override
    public String getPath() {
        return "/v1/dataframe_override_audits/update_failed?request_id=" + requestId;
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class);
    }

    @Override
    public RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        return requestBuilder;
    }
}

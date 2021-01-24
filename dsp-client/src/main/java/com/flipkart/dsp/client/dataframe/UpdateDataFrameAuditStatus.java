package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

public class UpdateDataFrameAuditStatus extends AbstractDSPServiceRequest<Void> {

    private final Long requestId;
    private final Long workFlowId;
    private final String currentStatus;
    private final String newStatus;

    public UpdateDataFrameAuditStatus(DSPServiceClient client, Long requestId,  Long workFlowId, String currentStatus, String newStatus) {
        super(client);
        this.requestId = requestId;
        this.workFlowId = workFlowId;
        this.currentStatus = currentStatus;
        this.newStatus = newStatus;
    }

    @Override
    protected String getMethod() {
        return "PATCH";
    }

    @Override
    protected String getPath() {
        return "/v1/dataframe_audits" + "/" + requestId + "/" + workFlowId + "/" + currentStatus + "/" + newStatus +  "/update";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        return requestBuilder;
    }
}

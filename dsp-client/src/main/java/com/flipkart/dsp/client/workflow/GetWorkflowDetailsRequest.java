package com.flipkart.dsp.client.workflow;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;


public class GetWorkflowDetailsRequest extends AbstractDSPServiceRequest<WorkflowDetails> {
    private Long workflowId;

    public GetWorkflowDetailsRequest(DSPServiceClient client, Long workflowId) {
        super(client);
        this.workflowId = workflowId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/workflow/details/" + workflowId;
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(WorkflowDetails.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-type", "application/json");
        return requestBuilder;
    }
}

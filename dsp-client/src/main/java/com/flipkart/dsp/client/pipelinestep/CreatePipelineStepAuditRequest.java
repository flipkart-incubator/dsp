package com.flipkart.dsp.client.pipelinestep;


import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
/**
 */
public class CreatePipelineStepAuditRequest extends AbstractDSPServiceRequest<PipelineStepAudit> {

    private final PipelineStepAudit pipelineStepAudit;


    public CreatePipelineStepAuditRequest(DSPServiceClient client, PipelineStepAudit pipelineStepAudit) {
        super(client);
        this.pipelineStepAudit = pipelineStepAudit;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/v1/pipeline_step_audit";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(PipelineStepAudit.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(pipelineStepAudit));
        return requestBuilder;
    }
}

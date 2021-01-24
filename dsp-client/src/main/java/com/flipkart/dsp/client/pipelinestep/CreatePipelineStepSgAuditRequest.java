package com.flipkart.dsp.client.pipelinestep;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepSGAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

public class CreatePipelineStepSgAuditRequest extends AbstractDSPServiceRequest<Long> {
    private final PipelineStepSGAudit pipelineStepSgAudit;


    public CreatePipelineStepSgAuditRequest(DSPServiceClient client, PipelineStepSGAudit pipelineStepSgAudit) {
        super(client);
        this.pipelineStepSgAudit = pipelineStepSgAudit;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/v1/pipeline_step_sg_audit";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Long.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(pipelineStepSgAudit));
        return requestBuilder;
    }
}

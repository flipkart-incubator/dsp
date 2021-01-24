package com.flipkart.dsp.client.pipelinestep;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.common.base.Preconditions;
import com.ning.http.client.RequestBuilder;

/**
 */
public class CreatePipelineStepRuntimeConfigRequest extends AbstractDSPServiceRequest<Void> {

    private PipelineStepRuntimeConfig pipelineStepRuntimeConfig;

    public CreatePipelineStepRuntimeConfigRequest(DSPServiceClient client, PipelineStepRuntimeConfig pipelineStepRuntimeConfig) {
        super(client);
        this.pipelineStepRuntimeConfig = pipelineStepRuntimeConfig;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/v1/pipelineSteps/runtimeConfigs";
    }

    @Override
    protected void validateReadiness() {
        super.validateReadiness();
        Preconditions.checkNotNull(pipelineStepRuntimeConfig.getWorkflowExecutionId(), "workflowExecutionId is required");
        Preconditions.checkNotNull(pipelineStepRuntimeConfig.getPipelineExecutionId(), "pipelineExecutionId is required");
        Preconditions.checkNotNull(pipelineStepRuntimeConfig.getPipelineStepId(), "pipelineStepId is required");
        Preconditions.checkNotNull(pipelineStepRuntimeConfig.getScope(), "scope is required");
        Preconditions.checkNotNull(pipelineStepRuntimeConfig.getRunConfig(), "runConfig is required");
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(pipelineStepRuntimeConfig));
        return requestBuilder;
    }
}

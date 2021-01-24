package com.flipkart.dsp.client.pipelinestep;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 */
public class GetPipelineStepRuntimeConfigRequest extends AbstractDSPServiceRequest<PipelineStepRuntimeConfig> {

    private final long pipelineStepId;
    private final String pipelineExecutionId;

    public GetPipelineStepRuntimeConfigRequest(DSPServiceClient client, String pipelineExecutionId, long pipelineStepId) {
        super(client);
        this.pipelineStepId = pipelineStepId;
        this.pipelineExecutionId = pipelineExecutionId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/pipelineSteps/runtimeConfigs";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(PipelineStepRuntimeConfig.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.addQueryParam("pipelineStepId", String.valueOf(pipelineStepId));
        requestBuilder.addQueryParam("pipelineExecutionId", String.valueOf(pipelineExecutionId));
        return requestBuilder;
    }
}

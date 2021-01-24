package com.flipkart.dsp.client.pipelinestep;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

import java.util.List;

/**
 */
public class GetPipelineStepRuntimeConfigByScopeRequest extends AbstractDSPServiceRequest<List<PipelineStepRuntimeConfig>> {

    private final String scope;
    private final String workflowExecutionId;

    public GetPipelineStepRuntimeConfigByScopeRequest(DSPServiceClient client, String workflowExecutionId, String scope) {
        super(client);
        this.scope = scope;
        this.workflowExecutionId = workflowExecutionId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/pipelineSteps/runtimeConfigs/scope";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructCollectionType(List.class, PipelineStepRuntimeConfig.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.addQueryParam("scope", String.valueOf(scope));
        requestBuilder.addQueryParam("workflowExecutionId", String.valueOf(workflowExecutionId));
        return requestBuilder;
    }
}

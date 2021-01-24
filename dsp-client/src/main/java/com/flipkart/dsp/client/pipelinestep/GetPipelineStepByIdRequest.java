package com.flipkart.dsp.client.pipelinestep;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 */
public class GetPipelineStepByIdRequest extends AbstractDSPServiceRequest<PipelineStep> {

    private final Long pipelineStepId;

    public GetPipelineStepByIdRequest(DSPServiceClient client, Long pipelineStepId) {
        super(client);
        this.pipelineStepId = pipelineStepId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/pipeline_step/" + pipelineStepId;
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(PipelineStep.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }
}

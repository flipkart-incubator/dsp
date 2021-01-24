package com.flipkart.dsp.client.pipelinestep;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

import java.util.List;
import java.util.Objects;

public class GetPipelineStepAuditsByPipelineExecutionIdRequest extends AbstractDSPServiceRequest<List<PipelineStepAudit>> {
    private final Integer attempt;
    private final Long refreshId;
    private final Long pipelineStepId;
    private final String pipelineExecutionId;

    public GetPipelineStepAuditsByPipelineExecutionIdRequest(DSPServiceClient client, Integer attempt, Long refreshId,
                                                             Long pipelineStepId, String pipelineExecutionId) {
        super(client);
        this.attempt = attempt;
        this.refreshId = refreshId;
        this.pipelineStepId = pipelineStepId;
        this.pipelineExecutionId = pipelineExecutionId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/pipeline_step_audit/pipelineStepDetails";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructCollectionType(List.class, PipelineStepAudit.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        if (Objects.nonNull(attempt)) requestBuilder.addQueryParam("attempt", String.valueOf(attempt));
        requestBuilder.addQueryParam("refreshId", String.valueOf(refreshId));
        requestBuilder.addQueryParam("pipelineStepId", String.valueOf(pipelineStepId));
        requestBuilder.addQueryParam("pipelineExecutionId", String.valueOf(pipelineExecutionId));
        return requestBuilder;
    }
}

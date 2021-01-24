package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateRequestDataFrameAuditsRequest extends AbstractDSPServiceRequest<Map<String, List<String>>> {

    private final Long requestId;
    private final Long workflowId;
    private final Long pipelineStepId;
    private final Set<DataFrameAudit> dataFrameAudits;

    public CreateRequestDataFrameAuditsRequest(DSPServiceClient client, Long requestId, Long workflowId,
                                               Long pipelineStepId, Set<DataFrameAudit> dataFrameAudits) {
        super(client);
        this.requestId = requestId;
        this.workflowId = workflowId;
        this.dataFrameAudits = dataFrameAudits;
        this.pipelineStepId = pipelineStepId;
    }

    @Override
    public String getMethod() {
        return "POST";
    }

    @Override
    public String getPath() {
        return "/v1/dataframe_audits" + "/" + requestId + "/" + workflowId + "/" + pipelineStepId +  "/create";
    }

    @Override
    public JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Map.class);
    }

    @Override
    public RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(dataFrameAudits));
        return requestBuilder;
    }
}

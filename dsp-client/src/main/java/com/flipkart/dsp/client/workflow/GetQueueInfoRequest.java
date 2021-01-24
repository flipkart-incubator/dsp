package com.flipkart.dsp.client.workflow;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.dto.QueueInfoDTO;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetQueueInfoRequest extends AbstractDSPServiceRequest<QueueInfoDTO> {
    private final Long workflowId;

    public GetQueueInfoRequest(DSPServiceClient dspServiceClient, Long workflowId) {
        super(dspServiceClient);
        this.workflowId = workflowId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/workflow/" + workflowId + "/queue";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(QueueInfoDTO.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        return requestBuilder;
    }
}
package com.flipkart.dsp.client.request;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 * +
 */
public class GetRequestStatusByRequestId extends AbstractDSPServiceRequest<RequestStatus> {
    private Long requestId;

    public GetRequestStatusByRequestId(DSPServiceClient dspServiceClient, Long requestId) {
        super(dspServiceClient);
        this.requestId = requestId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v2/requests/status/" + requestId;
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(RequestStatus.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }
}

package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 * +
 */
public class GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest extends AbstractDSPServiceRequest<DataFrameOverrideAudit> {
    private Long requestId;
    private Long dataFrameId;
    
    public GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest(DSPServiceClient dspServiceClient, Long dataFrameId, Long requestId) {
        super(dspServiceClient);
        this.dataFrameId = dataFrameId;
        this.requestId = requestId;
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    public String getPath() {
        return "/v1/dataframe_override_audits/dataframe_id/" + dataFrameId + "?request_id=" + requestId;
    }

    @Override
    public JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameOverrideAudit.class);
    }

    @Override
    public RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }
}

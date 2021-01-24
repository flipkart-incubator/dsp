package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 * +
 */
public class GetDataFrameAuditByIdRequest extends AbstractDSPServiceRequest<DataFrameAudit> {
    private Long dataFrameAuditId;

    public GetDataFrameAuditByIdRequest(DSPServiceClient dspServiceClient, Long dataFrameAuditId) {
        super(dspServiceClient);
        this.dataFrameAuditId = dataFrameAuditId;
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    public String getPath() {
        return "/v1/dataframe_audits/" + dataFrameAuditId;
    }

    @Override
    public JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameAudit.class);
    }

    @Override
    public RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }
}

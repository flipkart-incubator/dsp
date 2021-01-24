package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 * +
 */
public class GetLatestDataFrameAuditByDataFrameIdRequest extends AbstractDSPServiceRequest<DataFrameAudit> {
    private Long dataFrameId;

    public GetLatestDataFrameAuditByDataFrameIdRequest(DSPServiceClient dspServiceClient, Long dataFrameId) {
        super(dspServiceClient);
        this.dataFrameId = dataFrameId;
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    public String getPath() {
        return  "/v1/dataframe_audits/dataframe_id/" + dataFrameId;
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

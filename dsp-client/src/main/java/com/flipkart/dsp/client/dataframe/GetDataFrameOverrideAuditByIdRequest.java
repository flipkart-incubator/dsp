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
public class GetDataFrameOverrideAuditByIdRequest extends AbstractDSPServiceRequest<DataFrameOverrideAudit> {
    private Long id;
    
    public GetDataFrameOverrideAuditByIdRequest(DSPServiceClient dspServiceClient, Long id) {
        super(dspServiceClient);
        this.id = id;
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    public String getPath() {
        return "/v1/dataframe_override_audits/" + id;
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

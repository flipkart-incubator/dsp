package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

public class CreateDataFrameAuditRequest extends AbstractDSPServiceRequest<DataFrameAudit> {

    private final DataFrameAudit dataFrameAudit;

    public CreateDataFrameAuditRequest(DSPServiceClient client, DataFrameAudit dataFrameAudit) {
        super(client);
        this.dataFrameAudit = dataFrameAudit;
    }

    @Override
    public String getMethod() {
        return "POST";
    }

    @Override
    public String getPath() {
        return "/v1/dataframe_audits";
    }

    @Override
    public JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameAudit.class);
    }

    @Override
    public RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(dataFrameAudit));
        return requestBuilder;
    }
}

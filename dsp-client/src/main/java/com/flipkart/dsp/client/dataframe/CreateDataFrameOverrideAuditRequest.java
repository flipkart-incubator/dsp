package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

public class CreateDataFrameOverrideAuditRequest extends AbstractDSPServiceRequest<DataFrameOverrideAudit> {

    private final DataFrameOverrideAudit dataFrameOverrideAudit;

    public CreateDataFrameOverrideAuditRequest(DSPServiceClient client, DataFrameOverrideAudit dataFrameOverrideAudit) {
        super(client);
        this.dataFrameOverrideAudit = dataFrameOverrideAudit;
    }

    @Override
    public String getMethod() {
        return "POST";
    }

    @Override
    public String getPath() {
        return "/v1/dataframe_override_audits";
    }

    @Override
    public JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataFrameOverrideAudit.class);
    }

    @Override
    public RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(dataFrameOverrideAudit));
        return requestBuilder;
    }
}

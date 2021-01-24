package com.flipkart.dsp.client.dataframe;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 * +
 */
public class GetDataFrameOverrideAuditRequest extends AbstractDSPServiceRequest<DataFrameOverrideAudit> {
    private Long dataFrameId;
    private String inputDataId;
    private DataFrameOverrideType dataFrameOverrideType;

    public GetDataFrameOverrideAuditRequest(DSPServiceClient dspServiceClient, Long dataFrameId,
                                            String inputDataId, DataFrameOverrideType dataFrameOverrideType) {
        super(dspServiceClient);
        this.dataFrameId = dataFrameId;
        this.inputDataId = inputDataId;
        this.dataFrameOverrideType = dataFrameOverrideType;
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    public String getPath() {
        return "/v1/dataframe_override_audits/dataframe_id/" + dataFrameId + "?input_data_id=" + inputDataId + "&override_type=" + dataFrameOverrideType;
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

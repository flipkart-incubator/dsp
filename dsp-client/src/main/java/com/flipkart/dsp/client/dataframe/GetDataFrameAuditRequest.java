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
public class GetDataFrameAuditRequest extends AbstractDSPServiceRequest<DataFrameAudit> {
    private Long dataFrameId;
    private String partitions;
    private Long dataFrameOverrideAuditId;

    public GetDataFrameAuditRequest(DSPServiceClient dspServiceClient, Long dataFrameId,
                                    Long dataFrameOverrideAuditId, String partitions) {
        super(dspServiceClient);
        this.partitions = partitions;
        this.dataFrameId = dataFrameId;
        this.dataFrameOverrideAuditId = dataFrameOverrideAuditId;
    }

    @Override
    public String getMethod() {
        return "GET";
    }

    @Override
    public String getPath() {
        return  "/v1/dataframe_audits/dataframe_id/" + dataFrameId + "?partitions=" + partitions + "&override_audit_id=" + dataFrameOverrideAuditId;
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

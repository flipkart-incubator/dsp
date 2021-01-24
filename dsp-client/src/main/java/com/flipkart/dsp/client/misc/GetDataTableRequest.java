package com.flipkart.dsp.client.misc;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.sg.DataTable;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 * +
 */
public class GetDataTableRequest extends AbstractDSPServiceRequest<DataTable> {
    private String tableName;


    public GetDataTableRequest(DSPServiceClient dspServiceClient, String tableName) {
        super(dspServiceClient);
        this.tableName = tableName;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/data_tables/" + tableName;
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(DataTable.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }
}

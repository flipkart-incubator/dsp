package com.flipkart.dsp.client.misc;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 */
public class CreateExecutionEnvironmentSnapshotRequest extends AbstractDSPServiceRequest<Void> {
    private ExecutionEnvironmentSnapshot executionEnvironmentSnapShot;

    public CreateExecutionEnvironmentSnapshotRequest(DSPServiceClient client, ExecutionEnvironmentSnapshot executionEnvironmentSnapShot) {
        super(client);
        this.executionEnvironmentSnapShot = executionEnvironmentSnapShot;
    }

    @Override
    public String getMethod() {
        return "POST";
    }

    @Override
    public String getPath() {
        return "/v1/execution-environment-snapshots/create";
    }

    @Override
    public JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class);
    }

    @Override
    public RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(executionEnvironmentSnapShot));
        return requestBuilder;
    }
}

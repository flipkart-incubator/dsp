package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 */
public class AzkabanLoginRequest extends AbstractAzkabanRequest<AzkabanLoginResponse> {

    private AzkabanConfig azkabanConfig ;

    public AzkabanLoginRequest(AzkabanClient client) {
        super(client);
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(AzkabanLoginResponse.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.addHeader("Content-type", "application/x-www-form-urlencoded");
        requestBuilder.addFormParam("action", "login");
        requestBuilder.addFormParam("username", azkabanConfig.getUser());
        requestBuilder.addFormParam("password", azkabanConfig.getPassword());
        return requestBuilder;
    }

    public AzkabanLoginRequest azkabanConfig(AzkabanConfig azkabanConfig) {
        this.azkabanConfig = azkabanConfig;
        return this;
    }
}

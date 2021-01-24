package com.flipkart.dsp.client.script;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

import java.io.InputStream;

public class DownloadScriptFolderRequest extends AbstractDSPServiceRequest<InputStream> {

    private final long scriptId;

    public DownloadScriptFolderRequest(long scriptId, DSPServiceClient client) {
        super(client);
        this.scriptId = scriptId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/scripts/" + scriptId + "/download";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(InputStream.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }
}

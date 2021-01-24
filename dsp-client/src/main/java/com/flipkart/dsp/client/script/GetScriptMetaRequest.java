package com.flipkart.dsp.client.script;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.script.ScriptMeta;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 */
public class GetScriptMetaRequest extends AbstractDSPServiceRequest<ScriptMeta> {

    private final long scriptId;

    public GetScriptMetaRequest(DSPServiceClient client, long scriptId) {
        super(client);
        this.scriptId = scriptId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v1/scripts/" + scriptId;
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(ScriptMeta.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }
}

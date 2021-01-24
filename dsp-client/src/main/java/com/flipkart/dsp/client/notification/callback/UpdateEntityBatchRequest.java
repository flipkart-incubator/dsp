package com.flipkart.dsp.client.notification.callback;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.dto.UpdateEntityDTO;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

import java.util.List;

/**
 */
public class UpdateEntityBatchRequest extends AbstractDSPServiceRequest<Void> {

    private List<UpdateEntityDTO> updateEntityDTOList;

    public UpdateEntityBatchRequest(DSPServiceClient dspServiceClient, List<UpdateEntityDTO> updateEntityDTOList) {
        super(dspServiceClient);
        this.updateEntityDTOList = updateEntityDTOList;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/v1/callbacks/update_entity_batch";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(updateEntityDTOList));
        return requestBuilder;
    }
}

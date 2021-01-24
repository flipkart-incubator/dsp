package com.flipkart.dsp.client.notification.email;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.entities.misc.NotificationPreference;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 * +
 */
public class GetNotificationPreferenceRequest extends AbstractDSPServiceRequest<NotificationPreference> {
    private Long workflowId;

    public GetNotificationPreferenceRequest(DSPServiceClient dspServiceClient, Long workflowId) {
        super(dspServiceClient);
        this.workflowId = workflowId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/v2/notification_preference/" + workflowId;
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(NotificationPreference.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        return requestBuilder;
    }

}

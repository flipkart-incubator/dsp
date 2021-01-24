package com.flipkart.dsp.client.notification.email;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.AbstractDSPServiceRequest;
import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

/**
 */
public class SendEmailNotificationForPartitionStateChangeRequest extends AbstractDSPServiceRequest<Void> {

    private PartitionDetailsEmailNotificationRequest partitionDetailsEmailNotificationRequest;

    public SendEmailNotificationForPartitionStateChangeRequest(DSPServiceClient dspServiceClient,
                                                               PartitionDetailsEmailNotificationRequest partitionDetailsEmailNotificationRequest) {
        super(dspServiceClient);
        this.partitionDetailsEmailNotificationRequest = partitionDetailsEmailNotificationRequest;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/v1/email_notifications/partition_state_change/send";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        requestBuilder.setHeader("Content-Type", "application/json");
        requestBuilder.setBody(JsonUtils.DEFAULT.toJson(partitionDetailsEmailNotificationRequest));
        return requestBuilder;
    }
}

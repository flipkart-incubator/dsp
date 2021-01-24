package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;

public class AzkabanScheduleProjectRequest  extends AbstractAzkabanRequest<AzkabanProjectScheduleRequestResponse> {
    private String sessionId;
    private String isRecurring;
    private String period;
    private String projectName;
    private String flow;
    private Long projectId;
    private String scheduleTime;
    private String scheduleDate;

    public AzkabanScheduleProjectRequest(AzkabanClient client,
                                         String isRecurring,
                                         String period,
                                         String projectName,
                                         String flow,
                                         Long projectId,
                                         String scheduleTime,
                                         String scheduleDate,
                                         String sessionId) {
        super(client);
        this.isRecurring = isRecurring;
        this.period = period;
        this.projectName = projectName;
        this.flow = flow;
        this.projectId = projectId;
        this.scheduleTime = scheduleTime;
        this.scheduleDate = scheduleDate;
        this.sessionId = sessionId;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/schedule";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(AzkabanProjectScheduleRequestResponse.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        try {
            requestBuilder.addHeader("Content-type", "application/x-www-form-urlencoded");
            requestBuilder.addQueryParam("ajax", "scheduleFlow");
            requestBuilder.addQueryParam("is_recurring", isRecurring);
            requestBuilder.addQueryParam("period", period);
            requestBuilder.addQueryParam("projectName", projectName);
            requestBuilder.addQueryParam("flow", flow);
            requestBuilder.addQueryParam("projectId", String.valueOf(projectId));
            requestBuilder.addQueryParam("scheduleTime", scheduleTime);
            requestBuilder.addQueryParam("scheduleDate", scheduleDate);
            requestBuilder.addQueryParam("session.id", sessionId);
            return requestBuilder;
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while building request body", e);
        }
    }
}

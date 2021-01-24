package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.AzkabanClient;
import com.ning.http.client.RequestBuilder;

public class AzkabanCreateProjectRequest extends AbstractAzkabanRequest<AzkabanCreateProjectResponse> {
    private final String description;
    private final ObjectMapper objectMapper;
    private String sessionId;
    private String projectName;

    public AzkabanCreateProjectRequest(AzkabanClient client, ObjectMapper objectMapper, String sessionId, String projectName, String description) {
        super(client);
        this.objectMapper = objectMapper;
        this.sessionId = sessionId;
        this.projectName = projectName;
        this.description = description;
    }

    @Override
    protected String getMethod() {
        return "POST";
    }

    @Override
    protected String getPath() {
        return "/manager";
    }

    @Override
    protected JavaType getReturnType() {
        return objectMapper.getTypeFactory().constructType(AzkabanCreateProjectResponse.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        try {
            requestBuilder.addHeader("Content-type", "application/x-www-form-urlencoded");

            requestBuilder.addQueryParam("action", "create").
                    addQueryParam("session.id", sessionId).
                    addQueryParam("name", projectName).
                    addQueryParam("description", description);
            return requestBuilder;
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while building request body", e);
        }
    }
}

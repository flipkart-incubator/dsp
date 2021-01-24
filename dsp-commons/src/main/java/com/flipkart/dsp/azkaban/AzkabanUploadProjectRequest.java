package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.AzkabanClient;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.multipart.FilePart;
import com.ning.http.client.multipart.StringPart;

import java.io.File;

public class AzkabanUploadProjectRequest extends AbstractAzkabanRequest<Void> {
    private final String zipFilePath;
    private String sessionId;
    private String projectName;

    public AzkabanUploadProjectRequest(AzkabanClient client, String sessionId, String projectName, String zipFilePath) {
        super(client);
        this.sessionId = sessionId;
        this.projectName = projectName;
        this.zipFilePath = zipFilePath;
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
        return null;
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        try {
            requestBuilder.addHeader("Content-type", "multipart/mixed");

            requestBuilder
                    .addBodyPart(new FilePart("file", new File(zipFilePath), "application/zip"))
                    .addBodyPart(new StringPart("ajax", "upload"))
                    .addBodyPart(new StringPart("session.id", sessionId))
                    .addBodyPart(new StringPart("project", projectName));

            return requestBuilder;
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while building request body", e);
        }
    }
}

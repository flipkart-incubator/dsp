package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.ning.http.client.RequestBuilder;

/**
 */
public abstract class AbstractAzkabanRequest<T> implements AzkabanRequest<T> {

    protected final AzkabanClient client;
    protected ObjectMapper objectMapper = new ObjectMapper();

    public AbstractAzkabanRequest(AzkabanClient client) {
        this.client = client;
    }

    @Override
    public T executeSync() throws AzkabanException {
        return client.executesSync(getMethod(), getPath(), getReturnType(), this::buildRequest);
    }

    protected abstract String getMethod();
    protected abstract String getPath();
    protected abstract JavaType getReturnType();
    protected abstract RequestBuilder buildRequest(RequestBuilder requestBuilder);

}

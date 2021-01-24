package com.flipkart.dsp.client.misc;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.DSPServiceClient;
import com.ning.http.client.RequestBuilder;

import java.util.function.Function;

/**
 */
public abstract class AbstractDSPServiceRequest<T> implements DSPServiceRequest<T> {

    protected final DSPServiceClient client;

    public AbstractDSPServiceRequest(DSPServiceClient client) {
        this.client = client;
    }

    @Override
    public T executeSync() {
        return client.executesSync(getMethod(), getPath(), getReturnType(), this::buildRequest);
    }

    protected abstract String getMethod();
    protected abstract String getPath();
    protected abstract JavaType getReturnType();
    protected void validateReadiness() {}
    protected abstract RequestBuilder buildRequest(RequestBuilder requestBuilder);

    protected void addFormParam(RequestBuilder requestBuilder, String key, String value) {
        if (value != null) {
            requestBuilder.addFormParam(key, value);
        }
    }

    protected <V> void addFormParam(RequestBuilder requestBuilder, String key, V value, Function<V, String> transform) {
        if (value != null) {
            String transformedValue = transform.apply(value);
            addFormParam(requestBuilder, key, transformedValue);
        }
    }

    protected void addQueryParam(RequestBuilder requestBuilder, String key, String value) {
        if (value != null) {
            requestBuilder.addQueryParam(key, value);
        }
    }

    protected <V> void addQueryParam(RequestBuilder requestBuilder, String key, V value, Function<V, String> transform) {
        if (value != null) {
            String transformedValue = transform.apply(value);
            addQueryParam(requestBuilder, key, transformedValue);
        }
    }
}

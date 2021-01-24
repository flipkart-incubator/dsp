package com.flipkart.dsp.client.misc;

/**
 */
public interface DSPServiceRequest<T> {
    T executeSync() throws Exception;
}

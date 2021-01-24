package com.flipkart.dsp.azkaban;

/**
 */
public interface AzkabanRequest<T> {
    T executeSync() throws Exception;
}

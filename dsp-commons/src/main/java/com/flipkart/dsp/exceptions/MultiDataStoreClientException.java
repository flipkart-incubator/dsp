package com.flipkart.dsp.exceptions;

public class MultiDataStoreClientException extends Exception {
    public MultiDataStoreClientException(String message) {
        super(message);
    }

    public MultiDataStoreClientException(String message, Throwable cause) {
        super(message, cause);
    }
}

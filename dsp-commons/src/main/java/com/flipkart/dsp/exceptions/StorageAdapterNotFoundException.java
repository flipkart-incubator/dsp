package com.flipkart.dsp.exceptions;

public class StorageAdapterNotFoundException extends Exception {
    public StorageAdapterNotFoundException(String message) {
        super(message);
    }

    public StorageAdapterNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}

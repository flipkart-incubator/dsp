package com.flipkart.dsp.exception;

public class AzkabanProjectCreationException extends Exception{
    public AzkabanProjectCreationException(String message) {
        super(message);
    }

    public AzkabanProjectCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}

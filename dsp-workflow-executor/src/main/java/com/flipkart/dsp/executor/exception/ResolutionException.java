package com.flipkart.dsp.executor.exception;

public class ResolutionException extends Exception {
    public ResolutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResolutionException(String message) {
        super(message);
    }
}

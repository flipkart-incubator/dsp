package com.flipkart.dsp.mesos.exceptions;

public class TransformationException extends Exception {
    public TransformationException(String message) {
        super("Transformation Failed: " + message);
    }

    public TransformationException(String message, Throwable cause) {
        super("Transformation Failed: " + message, cause);
    }
}

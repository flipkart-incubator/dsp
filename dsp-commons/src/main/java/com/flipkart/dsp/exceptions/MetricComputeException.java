package com.flipkart.dsp.exceptions;

/**
 */
public class MetricComputeException extends RuntimeException {

    public MetricComputeException(String message) {
        super(message);
    }

    public MetricComputeException(String message, Throwable cause) {
        super(message, cause);
    }
}

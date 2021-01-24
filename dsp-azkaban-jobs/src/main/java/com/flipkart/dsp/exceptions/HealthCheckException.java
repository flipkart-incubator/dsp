package com.flipkart.dsp.exceptions;

/**
 */
public class HealthCheckException extends Exception {
    public HealthCheckException(String msg) {
        super(msg);
    }

    public HealthCheckException(String msg, Exception e) {
        super(msg, e);
    }
}

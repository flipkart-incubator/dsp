package com.flipkart.dsp.qe.exceptions;

/**
 */
public class HiveClientException extends Exception {
    public HiveClientException(String message) {
        super(message);
    }

    public HiveClientException(String message,Exception e) {
        super(message,e);
    }
}

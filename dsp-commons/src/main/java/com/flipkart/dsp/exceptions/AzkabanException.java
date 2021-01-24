package com.flipkart.dsp.exceptions;

/**
 */
public class AzkabanException extends Exception {
    public AzkabanException(String msg) {
        super(msg);
    }

    public AzkabanException(String msg, Exception e) {
        super(msg, e);
    }
}

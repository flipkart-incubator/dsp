package com.flipkart.dsp.exception;

/**
 */
public class DSPSvcException extends Exception {
    public DSPSvcException(String msg) {
        super(msg);
    }

    public DSPSvcException(String msg, Exception e) {
        super(msg, e);
    }
}

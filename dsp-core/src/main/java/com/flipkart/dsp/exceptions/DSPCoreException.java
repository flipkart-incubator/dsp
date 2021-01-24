package com.flipkart.dsp.exceptions;

public class DSPCoreException extends RuntimeException {
    public DSPCoreException(String msg) {
        super(msg);
    }

    public DSPCoreException(String msg, Exception e) {
        super(msg, e);
    }
}

package com.flipkart.dsp.client.exceptions;

public class DSPClientProcessingException extends Exception {
    public DSPClientProcessingException(String msg) {
        super(msg);
    }

    public DSPClientProcessingException(String msg, Exception e) {
        super(msg, e);
    }
}

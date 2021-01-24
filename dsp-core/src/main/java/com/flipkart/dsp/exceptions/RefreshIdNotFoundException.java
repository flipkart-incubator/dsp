package com.flipkart.dsp.exceptions;

public class RefreshIdNotFoundException extends Exception {
    public RefreshIdNotFoundException(String msg) {
        super(msg);
    }

    public RefreshIdNotFoundException(String msg, Exception e) {
        super(msg, e);
    }
}

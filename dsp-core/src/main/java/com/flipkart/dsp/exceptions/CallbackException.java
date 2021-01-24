package com.flipkart.dsp.exceptions;

public class CallbackException extends RuntimeException  {
    public CallbackException(String message) { super(message); }

    public CallbackException(String msg, Exception e) {
        super(msg, e);
    }
}

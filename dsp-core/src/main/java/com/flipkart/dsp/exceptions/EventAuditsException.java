package com.flipkart.dsp.exceptions;

public class EventAuditsException extends Exception {
    public EventAuditsException(String msg) {
        super(msg);
    }
    public EventAuditsException(String msg, Exception e) {
        super(msg, e);
    }
}

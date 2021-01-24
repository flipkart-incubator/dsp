package com.flipkart.dsp.exceptions;

/**
 */
public class RequestException extends Exception {
    public RequestException(String msg) {
        super(msg);
    }

    public RequestException(String msg, Exception e) {
        super(msg,e);
    }

    public RequestException(Throwable cause) {
        super(cause);
    }
}

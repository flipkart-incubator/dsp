package com.flipkart.dsp.exceptions;

import com.codahale.metrics.annotation.ExceptionMetered;

@ExceptionMetered
public class AzkabanException  extends RuntimeException {
    public AzkabanException(Throwable cause) {
        super(cause);
    }

    public AzkabanException(String msg) {
        super(msg);
    }

    public AzkabanException(String msg, Exception e) {
        super(msg, e);
    }
}

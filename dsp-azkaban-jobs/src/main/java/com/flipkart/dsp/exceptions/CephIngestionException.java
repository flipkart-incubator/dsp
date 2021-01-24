package com.flipkart.dsp.exceptions;

import com.codahale.metrics.annotation.ExceptionMetered;

/**
 * +
 */
@ExceptionMetered

public class CephIngestionException extends RuntimeException {
    public CephIngestionException(String msg) {
        super(msg);
    }

    public CephIngestionException(String message, Throwable cause) {
        super(message, cause);
    }
}



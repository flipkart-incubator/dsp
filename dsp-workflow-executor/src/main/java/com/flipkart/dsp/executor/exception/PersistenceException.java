package com.flipkart.dsp.executor.exception;

import com.codahale.metrics.annotation.ExceptionMetered;

@ExceptionMetered
public class PersistenceException extends Exception {
    public PersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}

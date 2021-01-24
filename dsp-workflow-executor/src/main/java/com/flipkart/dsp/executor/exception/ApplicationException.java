package com.flipkart.dsp.executor.exception;

import com.codahale.metrics.annotation.ExceptionMetered;

@ExceptionMetered
public class ApplicationException extends Exception {
    public ApplicationException(String appName, Throwable cause) {
        super("Following error encountered while running Application: " + appName, cause);
    }
}

package com.flipkart.dsp.executor.exception;

import com.codahale.metrics.annotation.ExceptionMetered;

@ExceptionMetered
public class AuditVariableResolutionException extends ResolutionException {
    public AuditVariableResolutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuditVariableResolutionException(String message) {
        super(message);
    }
}

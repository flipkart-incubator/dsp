package com.flipkart.dsp.executor.exception;

import com.codahale.metrics.annotation.ExceptionMetered;

@ExceptionMetered
public class DataframeResolutionException extends ResolutionException {

    public DataframeResolutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataframeResolutionException(String message) {
        super(message);
    }
}

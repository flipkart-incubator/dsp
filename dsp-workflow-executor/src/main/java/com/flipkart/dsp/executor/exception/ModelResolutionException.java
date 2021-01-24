package com.flipkart.dsp.executor.exception;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.flipkart.dsp.models.ScriptVariable;

@ExceptionMetered
public class ModelResolutionException extends ResolutionException {
    public ModelResolutionException(ScriptVariable scriptVariable, Throwable cause) {
        super("Failed to resolve Model:" + scriptVariable.toString(), cause);
    }

    public ModelResolutionException(String message) {
        super(message);
    }
}

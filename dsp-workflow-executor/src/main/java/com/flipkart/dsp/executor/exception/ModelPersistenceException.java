package com.flipkart.dsp.executor.exception;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.flipkart.dsp.models.ScriptVariable;

@ExceptionMetered
public class ModelPersistenceException extends PersistenceException {
    public ModelPersistenceException(ScriptVariable scriptVariable, Throwable cause) {
        super("Failed to persist scriptVariable: " + scriptVariable, cause);
    }
}

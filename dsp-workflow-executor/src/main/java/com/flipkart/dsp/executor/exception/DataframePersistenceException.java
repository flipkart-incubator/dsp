package com.flipkart.dsp.executor.exception;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.flipkart.dsp.models.ScriptVariable;

@ExceptionMetered
public class DataframePersistenceException extends PersistenceException {
    public DataframePersistenceException(ScriptVariable scriptVariable, Throwable cause) {
        super("Failed to persist scriptVariable: " + scriptVariable, cause);
    }
}

package com.flipkart.dsp.exception;

public class ScriptValidationException extends Exception{
    public ScriptValidationException(String message) {
        super(message);
    }

    public ScriptValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}

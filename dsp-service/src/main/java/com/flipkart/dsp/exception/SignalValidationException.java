package com.flipkart.dsp.exception;

public class SignalValidationException extends Exception{
    public SignalValidationException(String message) {
        super(message);
    }

    public SignalValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}

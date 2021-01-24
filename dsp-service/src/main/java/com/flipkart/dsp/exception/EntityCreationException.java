package com.flipkart.dsp.exception;

public class EntityCreationException extends Exception {
    public EntityCreationException(String message) {
        super(message);
    }

    public EntityCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}

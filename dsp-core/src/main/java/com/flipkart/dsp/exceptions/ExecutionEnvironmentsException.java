package com.flipkart.dsp.exceptions;

public class ExecutionEnvironmentsException extends Exception {
    public ExecutionEnvironmentsException(String message,Throwable cause) {
        super(message, cause);
    }

    public ExecutionEnvironmentsException(String message) {
        super(message);
    }
}

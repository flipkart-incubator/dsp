package com.flipkart.dsp.exception;

public class DataFrameCreationException extends RuntimeException{
    public DataFrameCreationException(String message) {
        super(message);
    }

    public DataFrameCreationException(String message, Throwable cause) {
        super(message, cause);
    }

}

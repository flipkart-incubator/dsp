package com.flipkart.dsp.sg.exceptions;

/**
 */
public class DataFrameGeneratorException extends Exception {
    public DataFrameGeneratorException(String msg) {
        super(msg);
    }

    public DataFrameGeneratorException(String msg, Exception e) {
        super(msg, e);
    }
}

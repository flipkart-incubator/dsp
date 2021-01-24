package com.flipkart.dsp.sg.exceptions;

/**
 */

public class HiveGeneratorException extends DataFrameGeneratorException {
    public HiveGeneratorException(String msg) {
        super(msg);
    }

    public HiveGeneratorException(String msg, Exception e) {
        super(msg, e);
    }
}

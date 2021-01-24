package com.flipkart.dsp.exceptions;

/**
 */
public class HDFSUtilsException extends Exception {
    public HDFSUtilsException(String msg) {
        super(msg);
    }
    public HDFSUtilsException(String msg, Exception e) {
        super(msg, e);
    }
}

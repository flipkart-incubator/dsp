package com.flipkart.dsp.qe.exceptions;

public class TableNotFoundException extends Exception {
    private String table;

    public TableNotFoundException(String table, String message) {
        super(message);
    }

    public TableNotFoundException(String table, String message, Throwable cause) {
        super(message, cause);
    }

}

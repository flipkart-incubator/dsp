package com.flipkart.exception;

public class TestBedException extends Exception {
    public TestBedException(String msg, Exception e) {
        super(msg, e);
    }
    public TestBedException(String msg) {
        super(msg);
    }
}

package com.flipkart.dsp.exceptions;

public class MetaStoreException extends RuntimeException  {
    public MetaStoreException(String message) { super(message); }

    public MetaStoreException(String msg, Exception e) {
        super(msg, e);
    }
}

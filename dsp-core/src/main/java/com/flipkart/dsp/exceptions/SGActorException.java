package com.flipkart.dsp.exceptions;

/**
 */

public class SGActorException extends Exception {
    public SGActorException(Class klass, String msg) {
        super(klass.getName() + "Exception : " + msg);
    }

    public SGActorException(String msg) {
        super(msg);
    }

    public SGActorException(Class klass, Exception e) {
        super(klass.getName() + "Exception : ", e);
    }

    public SGActorException(Class klass, String msg, Exception e) {
        super(klass.getName() + "Exception : " + msg, e);
    }

}

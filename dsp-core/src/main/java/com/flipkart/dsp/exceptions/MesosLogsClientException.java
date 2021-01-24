package com.flipkart.dsp.exceptions;

public class MesosLogsClientException extends Exception {
    public MesosLogsClientException(String message) {
        super(message);
    }

    public MesosLogsClientException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.flipkart.dsp.exceptions;

public class PartitionCountMismatchException extends RuntimeException {
    public PartitionCountMismatchException(String message) {
        super(message);
    }

    public PartitionCountMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}

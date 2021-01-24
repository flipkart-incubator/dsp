package com.flipkart.dsp.exceptions;

public class PipelineStepMissingException extends RuntimeException {
    public PipelineStepMissingException(String message) {
        super(message);
    }
}

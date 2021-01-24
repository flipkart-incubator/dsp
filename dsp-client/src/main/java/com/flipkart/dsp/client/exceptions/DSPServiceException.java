package com.flipkart.dsp.client.exceptions;

import com.flipkart.dsp.dto.Error;

/**
 */
public class DSPServiceException extends RuntimeException {
    private final Error error;

    public DSPServiceException(Error error) {
        super(error.getErrorMsg());
        this.error = error;
    }
}

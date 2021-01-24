package com.flipkart.dsp.entities.enums;

/**
 * +
 */
public enum ExternalHealthCheckStatus {

    SUCCESSFUL("SUCCESSFUL"),
    FAILED("FAILED");

    private final String text;

    ExternalHealthCheckStatus(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}

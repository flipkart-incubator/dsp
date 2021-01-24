package com.flipkart.dsp.entities.enums;

public enum BlobStatus {

    STARTED("STARTED"),
    COMPLETED("COMPLETED"),
    FAILED("FAILED"),
    DELETED("DELETED");

    private final String text;

    BlobStatus(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}

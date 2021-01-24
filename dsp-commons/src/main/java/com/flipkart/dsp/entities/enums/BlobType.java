package com.flipkart.dsp.entities.enums;

public enum  BlobType {

    R("R"),
    PY3("PY3");

    private final String text;

    BlobType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}

package com.flipkart.dsp.entities.enums;

public enum AzkabanConcurrentOption {

    PIPELINE("pipeline"),
    SKIP("skip"),
    QUEUE("queue");

    private String value;

    AzkabanConcurrentOption(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}

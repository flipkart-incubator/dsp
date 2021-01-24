package com.flipkart.dsp.entities.enums;

/**
 */
public enum RequestStepType {
    SG("SG"),
    WF("WF"),
    OI("OI"),
    TERMINAL("TERMINAL");

    private final String text;

    RequestStepType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}

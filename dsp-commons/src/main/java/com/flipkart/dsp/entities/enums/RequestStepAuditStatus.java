package com.flipkart.dsp.entities.enums;

/**
 * +
 */
public enum RequestStepAuditStatus {
    STARTED("STARTED"),
    FAILED("FAILED"),
    ENTITY_REGISTER("ENTITY_REGISTER"),
    FAILED_AT_SOURCE("FAILED_AT_SOURCE"),
    PROCESSING_FAIL("PROCESSING_FAIL"),
    SUCCESSFUL("SUCCESSFUL");

    private final String value;

    RequestStepAuditStatus(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }
}

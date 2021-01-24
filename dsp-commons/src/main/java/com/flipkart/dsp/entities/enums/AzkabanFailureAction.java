package com.flipkart.dsp.entities.enums;

public enum AzkabanFailureAction {

    FINISH_CURRENT("finishCurrent"),
    CANCEL_IMMEDIATELY("cancelImmediately"),
    FINISH_POSSIBLE("finishPossible");

    private String value;

    AzkabanFailureAction(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}

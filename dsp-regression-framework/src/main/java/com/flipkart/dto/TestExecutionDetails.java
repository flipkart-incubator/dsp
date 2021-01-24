package com.flipkart.dto;

import com.flipkart.enums.TestExecutionStatus;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class TestExecutionDetails {
    private String testScenarioName;
    private TestExecutionStatus testExecutionStatus;
    private String testDescription;
    private String failureReason;
    private Object extraDetails;
}

package com.flipkart.dto;

import com.flipkart.enums.TestExecutionStatus;
import com.flipkart.utils.CompareType;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class TestRunDetails {
    private Object actualResult;
    private CompareType compareType;
    private String failureReason;
    private String testDescription;
    private TestExecutionStatus testExecutionStatus;
    private Object extraDetails;
}

package com.flipkart.dsp.models.callback;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
public class WorkflowExecutionResult {
    private Map<String, List<ScriptExecutionResult>> scriptExecutionResultMap;
}

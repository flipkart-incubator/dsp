package com.flipkart.dsp.models.callback;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HDFSScriptExecutionResult implements ScriptExecutionResult {
    private String location;
}

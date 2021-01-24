package com.flipkart.dsp.models.callback;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HiveScriptExecutionResult implements ScriptExecutionResult {
    private String database;
    private String table;
    private Long refreshId;
}

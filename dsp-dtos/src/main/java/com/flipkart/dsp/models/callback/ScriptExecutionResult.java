package com.flipkart.dsp.models.callback;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CephScriptExecutionResult.class ,name="CephScriptExecutionResult"),
        @JsonSubTypes.Type(value = HDFSScriptExecutionResult.class ,name="HDFSScriptExecutionResult"),
        @JsonSubTypes.Type(value = HiveScriptExecutionResult.class ,name="HiveScriptExecutionResult")
})
public interface ScriptExecutionResult {
}

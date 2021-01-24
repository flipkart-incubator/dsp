package com.flipkart.dsp.actors.output_location;

import com.fasterxml.jackson.annotation.*;
import com.flipkart.dsp.models.callback.ScriptExecutionResult;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * +
 */
@Data
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CephOutputLocationActor.class, name = "CephOutputLocationActor"),
        @JsonSubTypes.Type(value = HiveOutputLocationActor.class, name = "HiveOutputLocationActor"),
})
public abstract class OutputLocationActor {
    public Long requestId;
    public Boolean onlyIngestionEntity;
    public OutputLocation outputLocation;

    public abstract ScriptExecutionResult getScriptExecutionResult();
}

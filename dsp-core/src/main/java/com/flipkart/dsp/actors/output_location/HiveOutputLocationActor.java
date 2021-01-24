package com.flipkart.dsp.actors.output_location;

import com.flipkart.dsp.models.callback.HiveScriptExecutionResult;
import com.flipkart.dsp.models.callback.ScriptExecutionResult;
import com.flipkart.dsp.models.outputVariable.HiveOutputLocation;
import com.flipkart.dsp.models.outputVariable.OutputLocation;

/**
 * +
 */
public class HiveOutputLocationActor extends OutputLocationActor {

    public HiveOutputLocationActor(Long requestId, boolean onlyIngestionEntity, OutputLocation outputLocation) {
        super(requestId, onlyIngestionEntity, outputLocation);
    }

    public ScriptExecutionResult getScriptExecutionResult() {
        if (!onlyIngestionEntity) {
            String database = ((HiveOutputLocation) outputLocation).getDatabase();
            String table = ((HiveOutputLocation) outputLocation).getTable();
            return new HiveScriptExecutionResult(database, table, requestId);
        }
        return null;
    }
}

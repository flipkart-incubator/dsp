package com.flipkart.dsp.actors.output_location;

import com.flipkart.dsp.models.callback.HDFSScriptExecutionResult;
import com.flipkart.dsp.models.callback.ScriptExecutionResult;
import com.flipkart.dsp.models.outputVariable.HDFSOutputLocation;
import com.flipkart.dsp.models.outputVariable.OutputLocation;

import java.util.Objects;

import static com.flipkart.dsp.utils.DBConstants.REFRESH_ID;

/**
 * +
 */
public class HDFSOutputLocationActor extends OutputLocationActor {
    private String hdfsBasePath;

    public HDFSOutputLocationActor(Long requestId, String hdfsBasePath, boolean onlyIngestionEntity, OutputLocation outputLocation) {
        super(requestId, onlyIngestionEntity, outputLocation);
        this.hdfsBasePath = hdfsBasePath;
    }

    public ScriptExecutionResult getScriptExecutionResult() {
        if (!onlyIngestionEntity) {
            HDFSOutputLocation hdfsOutputLocation = (HDFSOutputLocation) outputLocation;
            String hdfsLocation = Objects.isNull(hdfsOutputLocation.getLocation()) ? hdfsBasePath : hdfsOutputLocation.getLocation();
            hdfsLocation += REFRESH_ID + requestId;
            return new HDFSScriptExecutionResult(hdfsLocation);
        }
        return null;
    }
}

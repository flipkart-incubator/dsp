package com.flipkart.dsp.actors.output_location;

import com.flipkart.dsp.models.callback.CephScriptExecutionResult;
import com.flipkart.dsp.models.callback.ScriptExecutionResult;
import com.flipkart.dsp.models.externalentities.CephEntity;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import com.flipkart.dsp.utils.AmazonS3Utils;

/**
 * +
 */

public class CephOutputLocationActor extends OutputLocationActor {
    private String saltKey;
    private String workflowName;
    private CephEntity cephEntity;
    private String scriptVariableName;

    public CephOutputLocationActor(Long requestId, String saltKey, String workflowName, CephEntity cephEntity,
                                  boolean onlyIngestionEntity, String scriptVariableName, OutputLocation outputLocation) {
        super(requestId, onlyIngestionEntity, outputLocation);
        this.saltKey = saltKey;
        this.cephEntity = cephEntity;
        this.workflowName = workflowName;
        this.scriptVariableName = scriptVariableName;
    }
    public ScriptExecutionResult getScriptExecutionResult() {
        CephOutputLocation cephOutputLocation = (CephOutputLocation) outputLocation;
        String bucket = cephOutputLocation.getBucket();
        String path = AmazonS3Utils.getCephkey(requestId, cephOutputLocation.getPath(), workflowName, scriptVariableName);
        return new CephScriptExecutionResult(bucket, path,
                AmazonS3Utils.getCephUrls(saltKey, requestId, workflowName, scriptVariableName, cephEntity, cephOutputLocation));
    }
}

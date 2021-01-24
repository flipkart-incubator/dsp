package com.flipkart.dsp.actors;

import com.flipkart.dsp.actors.output_location.*;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.entities.enums.DSPWorkflowExecutionStatus;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.workflow.DSPWorkflowExecutionResult;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.CallbackException;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.models.callback.*;
import com.flipkart.dsp.models.externalentities.CephEntity;
import com.flipkart.dsp.models.outputVariable.*;
import com.flipkart.dsp.utils.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.*;

import static com.flipkart.dsp.models.RequestStatus.COMPLETED;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowExecutionResponseActor {
    private final DSPServiceConfig dspServiceConfig;
    private final ExternalCredentialsActor externalCredentialsActor;

    public Object getWorkflowExecutionPayload(Request request, RequestStatus requestStatus, WorkflowDetails workflowDetails) {
        final Optional<DSPWorkflowExecutionResult> dspWorkflowExecutionResult =
                getLegacyWorkflowExecutionResult(request, requestStatus, workflowDetails);

        if (dspWorkflowExecutionResult.isPresent()) return dspWorkflowExecutionResult.get();
        else return getNewWorkflowExecutionResult(request, workflowDetails, requestStatus, false);
    }

    private Optional<DSPWorkflowExecutionResult> getLegacyWorkflowExecutionResult(Request request, RequestStatus requestStatus,
                                                                                  WorkflowDetails workflowDetails) {
        DSPWorkflowExecutionStatus workflowExecutionStatus = COMPLETED.equals(requestStatus) ?
                DSPWorkflowExecutionStatus.SUCCEEDED : DSPWorkflowExecutionStatus.FAILED;

        if (Objects.nonNull(workflowDetails.getWorkflow().getWorkflowMeta().getCallbackEntities())
                && !workflowDetails.getWorkflow().getWorkflowMeta().getCallbackEntities().isEmpty()) {
            List<String> legacyCallbackTables = workflowDetails.getWorkflow().getWorkflowMeta().getCallbackEntities();
            DSPWorkflowExecutionResult workflowExecResult = new DSPWorkflowExecutionResult();
            workflowExecResult.setWorkflowExecutionStatus(workflowExecutionStatus);
            workflowExecResult.setRequestId(request.getRequestId());
            Map<String, Long> overridesMap = new HashMap<>();
            for (String tableName : legacyCallbackTables) {
                overridesMap.put(tableName, request.getId());
            }
            workflowExecResult.setPartitionOverrides(overridesMap);
            return Optional.of(workflowExecResult);
        } else {
            return Optional.empty();
        }
    }

    public WorkflowGroupExecutionResult getNewWorkflowExecutionResult(Request request, WorkflowDetails workflowDetails,
                                                                      RequestStatus requestStatus, boolean onlyIngestionEntity) {
        Map<String, WorkflowExecutionResult> workflowExecutionResultMap = new HashMap<>();
        Map<String, List<ScriptExecutionResult>> scriptExecutionResultMap = new HashMap<>();
        workflowDetails.getPipelineSteps().forEach(pipelineStep -> {
            final Script script = pipelineStep.getScript();
            script.getOutputVariables().forEach(scriptVariable -> {
                final List<OutputLocation> outputLocationDetailsList = scriptVariable.getOutputLocationDetailsList();
                if (!Objects.isNull(outputLocationDetailsList)) {
                    outputLocationDetailsList.forEach(outputLocation -> {
                        ScriptExecutionResult scriptExecutionResult = getScriptExecutionResult(request.getId(), onlyIngestionEntity,
                                workflowDetails.getWorkflow().getName(), scriptVariable, outputLocation);
                        updateScriptExecutionResultMap(scriptExecutionResultMap, scriptVariable, scriptExecutionResult);
                    });
                }
            });
        });
        workflowExecutionResultMap.put(workflowDetails.getWorkflow().getName(), new WorkflowExecutionResult(scriptExecutionResultMap));
        return new WorkflowGroupExecutionResult(request.getRequestId(), requestStatus,
                "Workflow Group Execution Terminated!", workflowExecutionResultMap);
    }

    private ScriptExecutionResult getScriptExecutionResult(Long requestId, boolean onlyIngestionEntity, String workflowName,
                                                           ScriptVariable scriptVariable, OutputLocation outputLocation) {
        return getOutputLocationActorObject(requestId, workflowName, onlyIngestionEntity, scriptVariable.getName(),
                outputLocation).getScriptExecutionResult();
    }

    private OutputLocationActor getOutputLocationActorObject(Long requestId, String workflowName, boolean onlyIngestionEntity,
                                                             String scriptVariableName, OutputLocation outputLocation) {
        if (outputLocation instanceof HiveOutputLocation)
            return new HiveOutputLocationActor(requestId, onlyIngestionEntity, outputLocation);
        else if (outputLocation instanceof HDFSOutputLocation)
            return new HDFSOutputLocationActor(requestId, dspServiceConfig.getHadoopConfig().getBasePath(), onlyIngestionEntity, outputLocation);
        else
            return new CephOutputLocationActor(requestId, dspServiceConfig.getMiscConfig().getSaltKey(), workflowName,
                    getCephEntityCredentials((CephOutputLocation) outputLocation), onlyIngestionEntity, scriptVariableName, outputLocation);
    }

    private CephEntity getCephEntityCredentials(CephOutputLocation cephOutputLocation) {
        try {
            String clientAlias = cephOutputLocation.getClientAlias();
            ExternalCredentials externalCredentials = externalCredentialsActor.getCredentials(clientAlias);
            return JsonUtils.DEFAULT.fromJson(externalCredentials.getDetails(), CephEntity.class);
        } catch (DSPCoreException e) {
            throw new CallbackException("Error in getting Ceph Entity. Error: " + e.getMessage());
        }
    }


    private void updateScriptExecutionResultMap(Map<String, List<ScriptExecutionResult>> scriptExecutionResultMap,
                                                ScriptVariable scriptVariable, ScriptExecutionResult scriptExecutionResult) {
        if (Objects.nonNull(scriptExecutionResult)) {
            if (scriptExecutionResultMap.containsKey(scriptVariable.getName())) {
                scriptExecutionResultMap.get(scriptVariable.getName()).add(scriptExecutionResult);
            } else {
                final ArrayList<ScriptExecutionResult> scriptExecutionResults = new ArrayList<ScriptExecutionResult>() {
                    {
                        add(scriptExecutionResult);
                    }
                };
                scriptExecutionResultMap.put(scriptVariable.getName(), scriptExecutionResults);
            }
        }
    }

}

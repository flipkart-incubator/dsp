package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.DataFrameActor;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.db.entities.SignalGroupEntity;
import com.flipkart.dsp.db.entities.SignalGroupToSignalEntity;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepResources;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import com.flipkart.dsp.models.sg.SignalGroup;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import lombok.RequiredArgsConstructor;

import javax.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.flipkart.dsp.utils.Constants.dot;

/**
 * +
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowVersionAPI {
    private final DataFrameActor dataFrameActor;

    public Double parseVersionToDouble(String version) {
        if (Objects.isNull(version))
            return null;
        return Double.parseDouble(version.substring(0, version.lastIndexOf(dot) -1));
    }

    public String parseVersionToString(Double version) {
        if (Objects.isNull(version)) return null;
        String versionAsText = Double.toString(version);
        int majorVersion = Integer.parseInt(versionAsText.split("\\.")[0]);
        int minorVersion = Integer.parseInt(versionAsText.split("\\.")[1]);
        return majorVersion + dot + minorVersion + dot + "0";
    }

    String getNewVersionForWorkflow(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails,
                                    List<DataFrameEntity> sgDataFrameEntities) {
        if (Objects.nonNull(existingWorkflowDetails)) {
            String majorVersion = existingWorkflowDetails.getWorkflow().getVersion().split("\\.")[0];
            String minorVersion = existingWorkflowDetails.getWorkflow().getVersion().split("\\.")[1];
            String patchVersion = existingWorkflowDetails.getWorkflow().getVersion().split("\\.")[2];
            return isSimilarMajorVersion(existingWorkflowDetails, createWorkflowDetails, sgDataFrameEntities)
                    ? isSimilarMinorVersion(existingWorkflowDetails, createWorkflowDetails)
                    ? isSimilarPatchVersion(existingWorkflowDetails, createWorkflowDetails)
                    ? existingWorkflowDetails.getWorkflow().getVersion() : majorVersion + dot + minorVersion + dot + (Integer.parseInt(patchVersion) + 1)
                    : majorVersion + dot + (Integer.parseInt(minorVersion) + 1) + dot + "0"
                    : (Integer.parseInt(majorVersion) + 1) + dot + "0" + dot + "0";
        }
        return "1.0.0";
    }

    private boolean isSimilarMajorVersion(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails,
                                          List<DataFrameEntity> sgDataFrameEntities) {
        return isSimilarPipelineSteps(existingWorkflowDetails, createWorkflowDetails) &&
                isSimilarDataFrame(existingWorkflowDetails, createWorkflowDetails, true) &&
                isSimilarInputDataFrameSchema(existingWorkflowDetails, sgDataFrameEntities) &&
                isSimilarDataFrame(existingWorkflowDetails, createWorkflowDetails, false) &&
                isSimilarPartitionGranularity(existingWorkflowDetails, createWorkflowDetails);
    }

    private boolean isSimilarPipelineSteps(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails) {
        return existingWorkflowDetails.getPipelineSteps().size() == createWorkflowDetails.getPipelineSteps().size() &&
                createWorkflowDetails.getPipelineSteps().stream().allMatch(currentPipelineStep -> existingWorkflowDetails.getPipelineSteps().stream()
                        .anyMatch(existingPipelineStep -> currentPipelineStep.getName().equalsIgnoreCase(existingPipelineStep.getName())));
    }

    private boolean isSimilarDataFrame(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails, boolean isInputVariable) {
        return existingWorkflowDetails.getPipelineSteps().stream().allMatch(existingPipelineStep ->
                createWorkflowDetails.getPipelineSteps().stream().anyMatch(currentPipelineStep ->
                        (isInputVariable && isSimilarScriptVariables(existingPipelineStep.getScript().getInputVariables(),
                                currentPipelineStep.getScript().getInputVariables(), true))
                                || (!isInputVariable && isSimilarScriptVariables(existingPipelineStep.getScript().getOutputVariables(),
                                currentPipelineStep.getScript().getOutputVariables(), false))));
    }

    private boolean isSimilarScriptVariables(Set<ScriptVariable> currentScriptVariables, Set<ScriptVariable> existingScriptVariables, boolean isInputVariable) {
        return currentScriptVariables.size() == existingScriptVariables.size()
                && currentScriptVariables.stream().noneMatch(currentScriptVariable -> existingScriptVariables.stream()
                .noneMatch(existingScriptVariable -> isSimilarScriptVariable(currentScriptVariable, existingScriptVariable, isInputVariable)));
    }

    private boolean isSimilarScriptVariable(ScriptVariable currentScriptVariable, ScriptVariable existingScriptVariable, boolean isInputVariable) {
        return existingScriptVariable.getName().equalsIgnoreCase(currentScriptVariable.getName())
                && existingScriptVariable.getDataType().equals(currentScriptVariable.getDataType())
                && isSimilarOutputLocations(currentScriptVariable, existingScriptVariable, isInputVariable);
    }

    private boolean isSimilarOutputLocations(ScriptVariable currentScriptVariable, ScriptVariable existingScriptVariable, boolean isInputVariable) {
        if (!isInputVariable && Objects.nonNull(currentScriptVariable.getOutputLocationDetailsList())
                && Objects.nonNull(existingScriptVariable.getOutputLocationDetailsList())) {
            List<OutputLocation> currentOutputLocations = currentScriptVariable.getOutputLocationDetailsList();
            List<OutputLocation> existingOutputLocations = existingScriptVariable.getOutputLocationDetailsList();
            return currentOutputLocations.size() == existingOutputLocations.size()
                    && currentOutputLocations.stream().allMatch(currentOutputLocation -> existingOutputLocations.stream()
                    .anyMatch(existingOutputLocation -> currentOutputLocation.getClass().getName().equalsIgnoreCase(existingOutputLocation.getClass().getName())
                            && currentOutputLocation.isSimilarOutputLocation(existingOutputLocation)));
        }
        return true;
    }

    private boolean isSimilarInputDataFrameSchema(WorkflowDetails workflowDetails, List<DataFrameEntity> sgDataFrameEntities) {
        return sgDataFrameEntities.stream().noneMatch(currentDataFrame -> workflowDetails.getWorkflow().getDataFrames().stream()
                .filter(existingDataFrame -> currentDataFrame.getName().equalsIgnoreCase(existingDataFrame.getName()))
                .noneMatch(existingDataFrame -> isAllColumnsSimilar(currentDataFrame.getSignalGroupEntity(), existingDataFrame.getSignalGroup())));
    }

    private boolean isAllColumnsSimilar(SignalGroupEntity signalGroupEntity, SignalGroup signalGroup) {
        List<SignalGroupToSignalEntity> currentSignals = signalGroupEntity.getSignalGroupToSignalEntities();
        List<SignalGroup.SignalMeta> existingSignals = signalGroup.getSignalMetas();

        return currentSignals.size() == existingSignals.size() && currentSignals.stream().noneMatch(currentSignal -> existingSignals.stream()
                .noneMatch(existingSignal -> currentSignal.getSignal().getName().equalsIgnoreCase(existingSignal.getSignal().getName())
                        && currentSignal.getSignal().getSignalDataType().equals(existingSignal.getSignal().getSignalDataType())));
    }

    private boolean isSimilarPartitionGranularity(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails) {
        return existingWorkflowDetails.getPipelineSteps().stream().noneMatch(existingPipelineStep ->
                createWorkflowDetails.getPipelineSteps().stream().noneMatch(currentPipelineStep ->
                        currentPipelineStep.getPartitions().size() == existingPipelineStep.getPartitions().size()
                                && isSimilarPartitions(currentPipelineStep.getPartitions(), existingPipelineStep.getPartitions())));
    }

    private boolean isSimilarPartitions(List<String> currentPartitions, List<String> existingPartitions) {
        return currentPartitions.stream().noneMatch(currentPartition -> existingPartitions.stream().noneMatch(currentPartition::equalsIgnoreCase));
    }

    private boolean isSimilarMinorVersion(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails) {
        return isSimilarExecutionEnvironment(existingWorkflowDetails, createWorkflowDetails)
                && isSimilarExecutionResources(existingWorkflowDetails, createWorkflowDetails)
                && isSimilarDataFormats(existingWorkflowDetails, createWorkflowDetails, true)
                && isSimilarDataFormats(existingWorkflowDetails, createWorkflowDetails, false)
                && isSimilarScripts(existingWorkflowDetails, createWorkflowDetails);
    }

    private boolean isSimilarExecutionEnvironment(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails) {
        for (PipelineStep existingPipelineStep : existingWorkflowDetails.getPipelineSteps()) {
            PipelineStep pipelineStep = createWorkflowDetails.getPipelineSteps().stream().filter(currentPipelineStep
                    -> existingPipelineStep.getName().equalsIgnoreCase(currentPipelineStep.getName())).findFirst().orElse(null);
            if (Objects.nonNull(pipelineStep)
                    && !pipelineStep.getScript().getExecutionEnvironment().equals(existingPipelineStep.getScript().getExecutionEnvironment()))
                return false;
        }
        return true;
    }

    private boolean isSimilarExecutionResources(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails) {
        for (PipelineStep existingPipelineStep : existingWorkflowDetails.getPipelineSteps()) {
            PipelineStep pipelineStep = createWorkflowDetails.getPipelineSteps().stream()
                    .filter(currentPipelineStep -> existingPipelineStep.getName().equalsIgnoreCase(currentPipelineStep.getName()))
                    .findFirst().orElse(null);
            if (Objects.nonNull(pipelineStep)
                    && !isSimilarResources(existingPipelineStep.getPipelineStepResources(), pipelineStep.getPipelineStepResources()))
                return false;
        }
        return true;
    }

    private boolean isSimilarResources(PipelineStepResources existingPipelineStepResources, PipelineStepResources currentPipelineStepResources) {
        return currentPipelineStepResources.getBaseCpu().intValue() == existingPipelineStepResources.getBaseCpu().intValue()
                && currentPipelineStepResources.getBaseMemory().intValue() == currentPipelineStepResources.getBaseMemory().intValue();
    }

    private boolean isSimilarDataFormats(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails, boolean isInputVariable) {
        for (PipelineStep existingPipelineStep : existingWorkflowDetails.getPipelineSteps()) {
            PipelineStep currentPipelineStep = createWorkflowDetails.getPipelineSteps().stream().filter(pipelineStep
                    -> existingPipelineStep.getName().equalsIgnoreCase(pipelineStep.getName())).findFirst().orElse(null);
            if (Objects.nonNull(currentPipelineStep) && (isInputVariable
                    && !isSimilarFormatScriptVariables(existingPipelineStep.getScript().getInputVariables(), currentPipelineStep.getScript().getInputVariables()) ||
                    !isSimilarFormatScriptVariables(existingPipelineStep.getScript().getOutputVariables(), currentPipelineStep.getScript().getOutputVariables())))
                return false;
        }
        return true;
    }

    private boolean isSimilarFormatScriptVariables(Set<ScriptVariable> existingScriptVariables, Set<ScriptVariable> currentScriptVariables) {
        for (ScriptVariable existingScriptVariable : existingScriptVariables) {
            ScriptVariable currentScriptVariable = currentScriptVariables.stream()
                    .filter(scriptVariable -> existingScriptVariable.getName().equalsIgnoreCase(scriptVariable.getName()))
                    .findFirst().orElse(null);
            if (Objects.nonNull(currentScriptVariable) && !isSimilarFormatScriptVariable(existingScriptVariable, currentScriptVariable))
                return false;
        }
        return true;
    }

    private boolean isSimilarFormatScriptVariable(ScriptVariable existingScriptVariable, ScriptVariable currentScriptVariable) {
        AbstractDataFrame existingDataFrameFormat = (AbstractDataFrame) existingScriptVariable.getAdditionalVariable();
        AbstractDataFrame currentDataFrameFormat = (AbstractDataFrame) currentScriptVariable.getAdditionalVariable();
        return Objects.nonNull(existingDataFrameFormat) && Objects.nonNull(currentDataFrameFormat)
                ? isSimilarValue(existingDataFrameFormat.getSeparator(), currentDataFrameFormat.getSeparator())
                && isSimilarValue(existingDataFrameFormat.getHeaderDataTypes(), currentDataFrameFormat.getHeaderDataTypes())
                && isSimilarValue(existingDataFrameFormat.getEncoding(), currentDataFrameFormat.getEncoding())
                && isSimilarValue(existingDataFrameFormat.getHeaders(), currentDataFrameFormat.getHeaders())
                && isSimilarValue(existingDataFrameFormat.getQuoteCharacter(), currentDataFrameFormat.getQuoteCharacter())
                && isSimilarValue(existingDataFrameFormat.getWithHeader(), currentDataFrameFormat.getWithHeader())
                && iSSimilarNaStrings(existingDataFrameFormat.getNaStrings(), currentDataFrameFormat.getNaStrings())
                : Objects.isNull(existingDataFrameFormat) && Objects.isNull(currentDataFrameFormat);
    }

    private boolean isSimilarValue(Object existingValue, Object currentValue) {
        return Objects.nonNull(existingValue) ? existingValue.equals(currentValue) : !Objects.nonNull(currentValue);
    }

    private boolean iSSimilarNaStrings(List<String> existingNaStrings, List<String> currentNaStrings) {
        return Objects.nonNull(existingNaStrings) ? Objects.nonNull(currentNaStrings) && existingNaStrings.size() == currentNaStrings.size()
                && existingNaStrings.stream().allMatch(currentNaString -> currentNaStrings.stream().anyMatch(currentNaString::equalsIgnoreCase))
                : !Objects.nonNull(currentNaStrings);
    }

    private boolean isSimilarScripts(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails) {
        for (PipelineStep existingPipelineStep : existingWorkflowDetails.getPipelineSteps()) {
            PipelineStep currentPipelineStep = createWorkflowDetails.getPipelineSteps().stream().filter(pipelineStep ->
                    existingPipelineStep.getName().equalsIgnoreCase(pipelineStep.getName())).findFirst().orElse(null);
            if (Objects.nonNull(currentPipelineStep)
                    && !isSimilarScript(existingPipelineStep.getScript(), currentPipelineStep.getScript()))
                return false;
        }
        return true;
    }

    private boolean isSimilarScript(Script existingScript, Script currentScript) {
        return isSimilarValue(existingScript.getGitRepo(), currentScript.getGitRepo())
                && isSimilarValue(existingScript.getGitFolder(), currentScript.getGitFolder())
                && isSimilarValue(existingScript.getGitCommitId(), currentScript.getGitCommitId())
                && isSimilarValue(existingScript.getFilePath(), currentScript.getFilePath());
    }

    private boolean isSimilarPatchVersion(WorkflowDetails existingWorkflowDetails, WorkflowDetails createWorkflowDetails) {
        return isSimilarValue(existingWorkflowDetails.getWorkflow().getDescription(), createWorkflowDetails.getWorkflow().getDescription());
    }

    String getNewProdWorkflowVersion(WorkflowDetails currentLatestProdWorkflow, WorkflowDetails workflowDetails) {
        List<DataFrameEntity> dataFrameEntities = workflowDetails.getWorkflow().getDataFrames().stream().map(dataFrameActor::unWrap).collect(Collectors.toList());
        String newVersion = getNewVersionForWorkflow(currentLatestProdWorkflow, workflowDetails, dataFrameEntities);
        if (Objects.nonNull(currentLatestProdWorkflow) &&  newVersion.equals(currentLatestProdWorkflow.getWorkflow().getVersion())) {
            String[] splits = currentLatestProdWorkflow.getWorkflow().getVersion().split("\\.");
            return splits[0] + dot  + splits[1] + dot +(Integer.parseInt(splits[2]) + 1);

        }
        return newVersion;
    }
}

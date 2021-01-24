package com.flipkart.dsp.entities.workflow;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import com.flipkart.dsp.models.outputVariable.HiveOutputLocation;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonSnakeCase
@Slf4j
public class WorkflowDetails implements Serializable{
    private Workflow workflow;
    private Long parentWorkflowRefreshId;
    private List<PipelineStep> pipelineSteps;

    @JsonIgnore
    public List<HiveOutputLocation> getHiveOutputLocations() {
        return pipelineSteps.stream()
                .flatMap(pipelineStep -> pipelineStep.getScript().getOutputVariables().stream())
                .filter(output -> (output.getDataType().equals(DataType.DATAFRAME))
                        && output.getOutputLocationDetailsList()!=null && !output.getOutputLocationDetailsList().isEmpty())
                .flatMap(scriptVariable -> scriptVariable.getOutputLocationDetailsList().stream())
                .filter(outputLocation -> outputLocation instanceof HiveOutputLocation)
                .map(outputLocation -> (HiveOutputLocation) outputLocation)
                .collect(toList());
    }

    @JsonIgnore
    public List<String> getLegacyOutputHiveTables() {
        return pipelineSteps.stream()
                .flatMap(ps -> ps.getScript().getOutputVariables().stream())
                .filter(v -> (v.getDataType().equals(DataType.DATAFRAME)) && (v.getAdditionalVariable() instanceof AbstractDataFrame))
                .map(v -> ((AbstractDataFrame) v.getAdditionalVariable()).getHiveTable())
                .collect(toList());
    }

    @JsonIgnore
    public List<ScriptVariable> getCephOutputs() {
        return pipelineSteps.stream()
                .flatMap(pipelineStep -> pipelineStep.getScript().getOutputVariables().stream())
                .filter(output -> (output.getDataType().equals(DataType.DATAFRAME))
                        && output.getOutputLocationDetailsList()!=null &&  !output.getOutputLocationDetailsList().isEmpty()
                        && output.getOutputLocationDetailsList().stream().anyMatch(outputLocation -> outputLocation instanceof CephOutputLocation))
                .collect(toList());
    }

}

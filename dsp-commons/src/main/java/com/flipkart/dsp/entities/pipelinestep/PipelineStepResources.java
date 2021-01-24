package com.flipkart.dsp.entities.pipelinestep;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown=true)
public class PipelineStepResources implements Serializable{
    @JsonProperty("BASE_MEMORY")
    private Long baseMemory = 8192L;

    @JsonProperty("BASE_CPU")
    private Double baseCpu = 1.0;

    @JsonProperty("TRAIN_MEM_COEFF")
    private Double trainingMemoryCoefficient = 0.0;

    @JsonProperty("EXEC_MEM_COEFF")
    private Double executionMemoryCoefficient = 0.0;

    @JsonProperty("TRAIN_CPU_COEFF")
    private Double trainingCpuCoefficient = 0.0;

    @JsonProperty("EXEC_CPU_COEFF")
    private Double executionCpuCoefficient = 0.0;
}

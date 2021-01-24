package com.flipkart.dsp.entities.pipelinestep;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.entities.script.Script;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 */
@Data
@Builder
@JsonSnakeCase
@JsonAutoDetect
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class PipelineStep implements Serializable {
    private Long id;
    private String name;
    private Long parentPipelineStepId;
    private Script script;
    private PipelineStepResources pipelineStepResources;
    private String prevStepName;
    private List<String> partitions;
}

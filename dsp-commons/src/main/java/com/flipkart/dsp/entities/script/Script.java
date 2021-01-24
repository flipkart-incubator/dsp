package com.flipkart.dsp.entities.script;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.models.ScriptVariable;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.*;

import java.io.Serializable;
import java.util.Set;

/**
 */
@Getter
@Setter
@Builder
@ToString
@JsonSnakeCase
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class Script implements Serializable{

    private Long id;
    private Double version;
    private String gitRepo;
    private Boolean isDraft;
    private String filePath;
    private String gitFolder;
    private String gitCommitId;
    private String executionEnvironment;
    private Set<ScriptVariable> inputVariables ;  // Only for setting objects like dataFrame, Model etc.
    private Set<ScriptVariable> outputVariables;
}

package com.flipkart.dsp.entities.run.config;

import com.flipkart.dsp.models.ScriptVariable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Set;

/**
 */

@AllArgsConstructor
@NoArgsConstructor
@Getter
@ToString
public class RunConfig {
    private Set<ScriptVariable> extractedVariableSet;
    private Set<ScriptVariable> persistedVariableSet;
}

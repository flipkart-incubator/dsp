package com.flipkart.dsp.executor.loader;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.models.ScriptVariable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@AllArgsConstructor
@Slf4j
public class VariableLoader {
    private ScriptExecutionEngine scriptExecutionEngine;

    @Timed
    @Metered
    public void load(Set<ScriptVariable> scriptVariables) throws ScriptExecutionEngineException {
        for (ScriptVariable scriptVariable : scriptVariables) {
            scriptExecutionEngine.assign(scriptVariable);
        }
    }
}

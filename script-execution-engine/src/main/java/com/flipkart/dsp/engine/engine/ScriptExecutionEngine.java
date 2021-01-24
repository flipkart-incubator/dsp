package com.flipkart.dsp.engine.engine;

import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.models.ScriptVariable;

/**
 */
public interface ScriptExecutionEngine {
    void shutdown();
    void assign(ScriptVariable scriptVariable) throws ScriptExecutionEngineException;

    void runScript(LocalScript script) throws ScriptExecutionEngineException;
    ScriptVariable extract(ScriptVariable scriptVariable) throws ScriptExecutionEngineException;
}

package com.flipkart.dsp.executor.utils;

import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.models.ScriptVariable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestScriptExecEngine implements ScriptExecutionEngine {
    @Override
    public void shutdown() {

    }

    @Override
    public void assign(ScriptVariable scriptVariable) {
        log.info("Test Script Engine !!! Assigned the variable !!!");
    }

    @Override
    public void runScript(LocalScript script) {
        log.info("Test Script Engine !!! Ran the script !!!");
    }

    @Override
    public ScriptVariable extract(ScriptVariable scriptVariable) {
        log.info("Test Script Engine !!! Extracted the variable !!!");
        return null;
    }
}

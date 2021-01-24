package com.flipkart.dsp.engine.engine;

import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.models.ScriptVariable;

public abstract class DataFrameHandler {

    protected void loadDataFrame(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        loadOnly(scriptVariable);
        deleteDataFrame(String.valueOf(scriptVariable.getValue()));
    }

    protected abstract void loadOnly(ScriptVariable scriptVariable) throws ScriptExecutionEngineException;

    protected abstract void deleteDataFrame(String path) throws ScriptExecutionEngineException;


}

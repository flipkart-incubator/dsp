package com.flipkart.dsp.engine.engine;

import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.engine.helper.RInputScriptGenerationHelper;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.variables.RDataFrame;
import org.junit.Assert;
import org.junit.Test;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import static org.mockito.Mockito.*;

public class RserveExecEngineTest {

    private final DSPServiceConfig.ScriptExecutionConfig config = mock(DSPServiceConfig.ScriptExecutionConfig.class);
    private final RConnection connection = mock(RConnection.class);
    private final RInputScriptGenerationHelper rInputScriptGenerationHelper = mock(RInputScriptGenerationHelper.class);
    private final RServeExecEngine rServeExecEngine = new RServeExecEngine(connection, rInputScriptGenerationHelper, config);

    @Test
    public void assignTest() throws ScriptExecutionEngineException, RserveException {
        String variableName = "variable1";
        String variableValue = "/path/to/file";
        ScriptVariable scriptVariable = new ScriptVariable(variableName, DataType.STRING, variableValue, new RDataFrame(), null, false);
        rServeExecEngine.assign(scriptVariable);
        verify(connection).assign(variableName, variableValue);
    }

    @Test
    public void extractTest() throws ScriptExecutionEngineException, REXPMismatchException, REngineException {
        String variableName = "variable1";
        String variableValue = "/path/to/file";
        ScriptVariable scriptVariable = new ScriptVariable(variableName, DataType.STRING, null, new RDataFrame(), null, false);
        when(config.isRedirectJRIConsoleToStdOut()).thenReturn(false);
        REXP rexp = mock(REXP.class);
        when(rexp.asString()).thenReturn(variableValue);
        when(connection.parseAndEval(variableName)).thenReturn(rexp);
        ScriptVariable extractedScriptVariable = rServeExecEngine.extract(scriptVariable);
        verify(connection).parseAndEval(variableName);
        Assert.assertEquals(variableValue, String.valueOf(extractedScriptVariable.getValue()));
    }

}

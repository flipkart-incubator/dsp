package com.flipkart.dsp.executor.extractor;

import com.flipkart.dsp.engine.engine.RServeExecEngine;
import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VariableExtractorTest {
    private ScriptVariable scriptVariable;
    private ScriptVariable outScriptVariable;
    private ScriptExecutionEngine scriptExecutionEngine = mock(RServeExecEngine.class);
    private LocationManager locationManager = mock(LocationManager.class);
    private VariableExtractor variableExtractor;

    @Before
    public void setup() throws ScriptExecutionEngineException {
        variableExtractor = new VariableExtractor(scriptExecutionEngine, locationManager);
        scriptVariable = new ScriptVariable("variable1", DataType.DATAFRAME, null, null, null, false);
        outScriptVariable = new ScriptVariable("variable1", DataType.DATAFRAME, "/path/to/dataframe", null, null, false);
        when(scriptExecutionEngine.extract(scriptVariable)).thenReturn(outScriptVariable);
    }

    @Test
    public void test1() throws ScriptExecutionEngineException {
        Set<ScriptVariable> scriptVariableSet = new HashSet<>();
        scriptVariableSet.add(scriptVariable);
        Set<ScriptVariable> actual = variableExtractor.extract(scriptVariableSet);
        Set<ScriptVariable> expected = new HashSet<>();
        expected.add(outScriptVariable);
        Assert.assertEquals(actual,expected);
    }
}

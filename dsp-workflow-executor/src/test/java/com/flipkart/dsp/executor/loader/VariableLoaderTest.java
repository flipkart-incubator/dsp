package com.flipkart.dsp.executor.loader;

import com.flipkart.dsp.engine.engine.RServeExecEngine;
import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class VariableLoaderTest {
    private VariableLoader variableLoader;
    private ScriptExecutionEngine scriptExecutionEngine = mock(RServeExecEngine.class);

    @Before
    public void setup() {
        variableLoader = new VariableLoader(scriptExecutionEngine);
    }

    @Test
    public void test() throws ScriptExecutionEngineException {
        ScriptVariable scriptVariable = new ScriptVariable("variable1", DataType.DATAFRAME, "/path/to/dataframe",null, null, false);
        variableLoader.load(Sets.newHashSet(scriptVariable));
        verify(scriptExecutionEngine).assign(scriptVariable);
    }
}

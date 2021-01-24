package com.flipkart.dsp.executor.runner;

import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.executor.extractor.VariableExtractor;
import com.flipkart.dsp.executor.loader.VariableLoader;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.ImageLanguageEnum;
import com.flipkart.dsp.models.ScriptVariable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.inject.Provider;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ScriptRunner.class, VariableLoader.class, VariableExtractor.class})
public class ScriptRunnerTest {
    private ScriptRunner scriptRunner;
    @Mock private LocalScript script;
    @Mock private VariableLoader variableLoader;
    @Mock private LocationManager locationManager;
    @Mock private VariableExtractor variableExtractor;
    @Mock private ScriptExecutionEngine scriptExecutionEngine;
    private Set<ScriptVariable> outputVariables = new HashSet<>();
    @Mock private Provider<ScriptExecutionEngine> scriptExecutionEngineProvider;
    @Mock private Map<ImageLanguageEnum, Provider<ScriptExecutionEngine>> scriptExecutionEngineMap;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        scriptRunner = spy(new ScriptRunner(locationManager, scriptExecutionEngineMap));
        outputVariables.add(ScriptVariable.builder().value("output").build());

        when(scriptExecutionEngineMap.get(any())).thenReturn(scriptExecutionEngineProvider);
        when(scriptExecutionEngineProvider.get()).thenReturn(scriptExecutionEngine);
    }

    // run(LocalScript, Set<ScriptVariable>)
    @Test
    public void testRunSuccessCase1() throws Exception {
        PowerMockito.whenNew(VariableLoader.class).withArguments(scriptExecutionEngine).thenReturn(variableLoader);
        PowerMockito.whenNew(VariableExtractor.class).withArguments(scriptExecutionEngine, locationManager).thenReturn(variableExtractor);
        doNothing().when(variableLoader).load(null);
        doNothing().when(scriptExecutionEngine).runScript(script);
        doReturn(outputVariables).when(variableExtractor).extract(anySet());

        Set<ScriptVariable> expected = scriptRunner.run(script, null);
        assertEquals(expected, outputVariables);
        assertEquals(expected.iterator().next().getValue().toString(), outputVariables.iterator().next().getValue().toString());

        verify(scriptExecutionEngineMap, times(1)).get(any());
        verify(scriptExecutionEngineProvider, times(1)).get();
        verify(variableLoader, times(1)).load(null);
        verify(scriptExecutionEngine, times(1)).runScript(script);
        verify(variableExtractor, times(1)).extract(anySet());
    }

    // run(LocalScript))
    @Test
    public void testRunSuccessCase2() throws Exception {
        doReturn(outputVariables).when(script).getOutputVariables();
        doNothing().when(scriptExecutionEngine).runScript(script);

        Set<ScriptVariable> expected = scriptRunner.run(script);
        assertEquals(expected, outputVariables);
        assertEquals(expected.iterator().next().getValue().toString(), outputVariables.iterator().next().getValue().toString());

        verify(scriptExecutionEngineMap, times(1)).get(any());
        verify(scriptExecutionEngineProvider, times(1)).get();
        verify(script, times(1)).getOutputVariables();
        verify(scriptExecutionEngine, times(1)).runScript(script);
    }
}

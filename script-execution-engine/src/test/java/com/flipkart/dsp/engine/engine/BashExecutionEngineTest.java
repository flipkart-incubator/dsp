package com.flipkart.dsp.engine.engine;

import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.models.ScriptVariable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BashExecutionEngine.class})
public class BashExecutionEngineTest {

    private Process process;
    private ProcessBuilder processBuilder;
    private LocalScript script = new LocalScript();
    private BashExecutionEngine bashExecutionEngine;
    private Set<ScriptVariable> inputScriptVariables = new HashSet<>();
    private InputStream inputStream = this.getClass().getResourceAsStream("/librarySet");

    @Before
    public void setUp() throws Exception {
        process = mock(Process.class);
        processBuilder = PowerMockito.mock(ProcessBuilder.class);
        bashExecutionEngine = spy(new BashExecutionEngine(processBuilder));

        String scriptLocation = "testScriptLocation";
        script.setLocation(scriptLocation);
        inputScriptVariables.add(ScriptVariable.builder().value("R").build());
        script.setInputVariables(inputScriptVariables);
        inputStream = this.getClass().getResourceAsStream("/librarySet");

        PowerMockito.whenNew(ProcessBuilder.class).withArguments(Mockito.anyString()).thenReturn(processBuilder);
        when(processBuilder.command(anyList())).thenReturn(processBuilder);
        when(processBuilder.start()).thenReturn(process);
        when(process.getInputStream()).thenReturn(inputStream);
    }

    @Test
    public void runScriptTestSuccess() throws Exception {
        when(process.waitFor()).thenReturn(0);
        bashExecutionEngine.runScript(script);
    }

    @Test()
    public void runScriptTestFailureCase1() throws Exception {
        boolean isException = false;
        when(process.waitFor()).thenReturn(1);
        try {
            bashExecutionEngine.runScript(script);
        } catch (ScriptExecutionEngineException e) {
            isException = true;
            assertEquals(e.getMessage(),"Error in running script for get ImageDetails");
        }

        assertTrue(isException);
    }

    @Test()
    public void runScriptTestFailureCase2() throws Exception {
        when(processBuilder.start()).thenThrow(new IOException("IO Error"));

        boolean isException = false;
        try {
            bashExecutionEngine.runScript(script);
        } catch (ScriptExecutionEngineException e) {
            isException = true;
            assertEquals(e.getMessage(),"IO Error");
        }

        assertTrue(isException);
    }

}

package com.flipkart.dsp.engine.config;

import com.flipkart.dsp.engine.engine.BashExecutionEngine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BashExecutionEngine.class, BashExecEngineProvider.class})
public class BashExecEngineProviderTest {
    private ProcessBuilder processBuilder;
    private BashExecutionEngine bashExecutionEngine;
    private BashExecEngineProvider bashExecEngineProvider;

    @Before
    public void setUp() {
        processBuilder = PowerMockito.mock(ProcessBuilder.class);
        bashExecutionEngine = mock(BashExecutionEngine.class);
        bashExecEngineProvider = spy(new BashExecEngineProvider(processBuilder));
    }

    @Test
    public void testGet() throws Exception {
        PowerMockito.whenNew(BashExecutionEngine.class).withArguments(processBuilder).thenReturn(bashExecutionEngine);
        BashExecutionEngine actual = bashExecEngineProvider.get();
        assertEquals(actual, bashExecutionEngine);
    }
}

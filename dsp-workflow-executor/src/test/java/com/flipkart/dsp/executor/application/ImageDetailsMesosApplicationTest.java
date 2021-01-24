package com.flipkart.dsp.executor.application;

import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.misc.ImageDetailPayload;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.executor.exception.ApplicationException;
import com.flipkart.dsp.executor.orchestrator.ImageDetailsOrchestrator;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.models.ImageLanguageEnum;
import com.flipkart.dsp.utils.JsonUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;


/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ImageDetailsMesosApplication.class, Script.class})
public class ImageDetailsMesosApplicationTest {

    @Mock private MiscConfig miscConfig;
    @Mock private LocalScript localScript;
    @Mock private ImageDetailsOrchestrator imageDetailsOrchestrator;


    private String payload;
    String[] args = new String[1];
    private ImageDetailPayload imageDetailPayload;
    private ImageDetailsMesosApplication imageDetailsMesosApplication;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.imageDetailsMesosApplication = spy(new ImageDetailsMesosApplication(miscConfig, imageDetailsOrchestrator));

        String scriptName = "testImageName";
        String scriptPath = "testImagePath";
        ExecutionEnvironmentSummary.Specification specification = ExecutionEnvironmentSummary.Specification.builder().language(ImageLanguageEnum.R).build();
        ExecutionEnvironmentSummary executionEnvironment = ExecutionEnvironmentSummary.builder().specification(specification).build();
        imageDetailPayload = ImageDetailPayload.builder().executionEnvironmentSummary(executionEnvironment).build();
        payload = JsonUtils.DEFAULT.mapper.writeValueAsString(imageDetailPayload);
        args[0] = payload;

        PowerMockito.whenNew(LocalScript.class).withNoArguments().thenReturn(localScript);
    }

    @Test
    public void testExecuteSuccess() throws Exception {
        doNothing().when(imageDetailsOrchestrator).run(localScript, imageDetailPayload);
        imageDetailsMesosApplication.execute(args);
        verify(imageDetailsOrchestrator, times(1)).run(localScript, imageDetailPayload);
    }

    @Test
    public void testExecuteFailureCase1() {
        boolean isException = false;
        String[] args = new String[1];
        args[0] = "{\"invalid_payload}";

        try {
            imageDetailsMesosApplication.execute(args);
        } catch (ApplicationException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Following error encountered while running Application: ImageDetailsMesosApplication"));
        }
        assertTrue(isException);
    }

    @Test
    public void testExecuteFailureCase2() throws Exception {
        doThrow(new ScriptExecutionEngineException("Error")).when(imageDetailsOrchestrator).run(localScript, imageDetailPayload);
        boolean isException = false;

        try {
            imageDetailsMesosApplication.execute(args);
        } catch (ApplicationException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Following error encountered while running Application: ImageDetailsMesosApplication"));
        }
        assertTrue(isException);
    }
}

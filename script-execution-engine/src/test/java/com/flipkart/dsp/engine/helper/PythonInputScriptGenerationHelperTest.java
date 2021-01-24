package com.flipkart.dsp.engine.helper;

import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.variables.PandasDataFrame;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.nio.file.Files;
import java.nio.file.Paths;

import static com.flipkart.dsp.engine.utils.Constants.*;
import static org.junit.Assert.*;

/**
 * +
 */
public class PythonInputScriptGenerationHelperTest {

    private String dir = "/tmp";
    private ScriptVariable scriptVariable;
    private String name  = "input_dataframe";
    private String value = dir + "/" + name;
    private String headerDataTypes = "dataTypes";
    private AbstractDataFrame additionalVariable = new PandasDataFrame();
    private PythonInputScriptGenerationHelper pythonInputScriptGenerationHelper;

    @Before
    public void setUp() {
        this.pythonInputScriptGenerationHelper = PowerMockito.spy(new PythonInputScriptGenerationHelper());

        additionalVariable.setWithHeader(true);
        additionalVariable.setHeaderDataTypes(headerDataTypes);
        scriptVariable = ScriptVariable.builder()
                .name(name).value(value).additionalVariable(additionalVariable)
                .build();
    }

    @Test
    public void testCreateScriptSuccess() throws Exception {
        pythonInputScriptGenerationHelper.createScript(scriptVariable);
        String content = new String(Files.readAllBytes(Paths.get(dir + TEMP + SLASH  + name + SLASH + CREATE_PYTHON_DATA_FRAME_FILE_NAME)));

        assertTrue(content.contains(name));
        assertTrue(content.contains(headerDataTypes));
        assertFalse(content.contains(PATH));
        assertFalse(content.contains(HEADERS));
        assertFalse(content.contains(SEPARATOR));
        assertFalse(content.contains(DATAFRAME_NAME));
        assertFalse(content.contains(ENCODING));
        assertFalse(content.contains(DATE_TYPE));
        assertFalse(content.contains(NA_VALUES));
        assertFalse(content.contains(QUOTE_CHAR));
    }

    @Test
    @Ignore
    public void testCreateScriptFailure() {
        boolean isException = false;
        value = "/unknown" + "/" + name;
        scriptVariable = ScriptVariable.builder()
                .name(name).value(value).additionalVariable(additionalVariable)
                .build();

        try {
            pythonInputScriptGenerationHelper.createScript(scriptVariable);
        } catch (Exception e) {
            isException = true;
            assertEquals(e.getMessage(), "Script Execution Engine failed because of following reason: Exception while creating read input script file for dataframe: " + name);
        }
        assertTrue(isException);
    }
}

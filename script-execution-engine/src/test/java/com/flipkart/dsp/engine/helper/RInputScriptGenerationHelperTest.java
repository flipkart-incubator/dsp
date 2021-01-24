package com.flipkart.dsp.engine.helper;

import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.variables.RDataFrame;
import com.flipkart.dsp.models.variables.RDataTable;
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
public class RInputScriptGenerationHelperTest {

    private String dir = "/tmp";
    private String name  = "input_dataframe";
    private String value = dir + "/" + name;
    private String headerDataTypes = "dataTypes";
    private ScriptVariable rDataTableScriptVariable;
    private ScriptVariable rDataFramescriptVariable;
    private AbstractDataFrame rDataTable = new RDataTable();
    private AbstractDataFrame rDataFrame = new RDataFrame();
    private RInputScriptGenerationHelper rInputScriptGenerationHelper;

    @Before
    public void setUp() {
        this.rInputScriptGenerationHelper = PowerMockito.spy(new RInputScriptGenerationHelper());

        rDataTable.setWithHeader(true);
        rDataFrame.setWithHeader(true);
        rDataTable.setHeaderDataTypes(headerDataTypes);
        rDataFrame.setHeaderDataTypes(headerDataTypes);

        rDataTableScriptVariable = ScriptVariable.builder()
                .name(name).value(value).additionalVariable(rDataTable)
                .build();

        rDataFramescriptVariable = ScriptVariable.builder()
                .name(name).value(value).additionalVariable(rDataFrame)
                .build();
    }

    @Test
    public void testCreateScriptSuccessCase1() throws Exception {
        rInputScriptGenerationHelper.createScript(rDataTableScriptVariable, true);
        String content = new String(Files.readAllBytes(Paths.get(dir + TEMP + SLASH  + name + SLASH + CREATE_R_DATA_TABLE_FILE_NAME)));

        assertTrue(content.contains(name));
        assertTrue(content.contains(headerDataTypes));
        assertTrue(content.contains("fread"));
        assertFalse(content.contains(PATH));
        assertFalse(content.contains(HEADERS));
        assertFalse(content.contains(SEPARATOR));
        assertFalse(content.contains(DATAFRAME_NAME));
        assertFalse(content.contains(FILL));
        assertFalse(content.contains(READ_TYPE));
        assertFalse(content.contains(NA_STRINGS));
        assertFalse(content.contains(COL_CLASSES));
    }

    @Test
    public void testCreateScriptSuccessCase2() throws Exception {
        rInputScriptGenerationHelper.createScript(rDataFramescriptVariable, false);
        String content = new String(Files.readAllBytes(Paths.get(dir + TEMP + SLASH  + name + SLASH + CREATE_R_DATA_TABLE_FILE_NAME)));

        assertTrue(content.contains(name));
        assertTrue(content.contains(headerDataTypes));
        assertTrue(content.contains("read.csv"));
        assertFalse(content.contains(PATH));
        assertFalse(content.contains(HEADERS));
        assertFalse(content.contains(SEPARATOR));
        assertFalse(content.contains(DATAFRAME_NAME));
        assertFalse(content.contains(FILL));
        assertFalse(content.contains(READ_TYPE));
        assertFalse(content.contains(NA_STRINGS));
        assertFalse(content.contains(COL_CLASSES));
    }

    @Test
    @Ignore
    public void testCreateScriptFailure() {
        boolean isException = false;
        value = "/unknown" + "/" + name;
        rDataTableScriptVariable = ScriptVariable.builder()
                .name(name).value(value).additionalVariable(rDataTable)
                .build();

        try {
            rInputScriptGenerationHelper.createScript(rDataTableScriptVariable, true);
        } catch (Exception e) {
            isException = true;
            assertEquals(e.getMessage(), "Script Execution Engine failed because of following reason: Exception while creating read input script file for dataframe: " + name);
        }
        assertTrue(isException);
    }
}

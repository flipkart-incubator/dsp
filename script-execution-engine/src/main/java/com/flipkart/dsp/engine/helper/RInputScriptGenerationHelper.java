package com.flipkart.dsp.engine.helper;

import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.variables.RDataFrame;
import com.flipkart.dsp.models.variables.RDataTable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static com.flipkart.dsp.engine.commands.RCommands.ASSIGN_DATATYPES;
import static com.flipkart.dsp.engine.utils.Constants.*;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

/**
 * +
 */
@Slf4j
@AllArgsConstructor
public class RInputScriptGenerationHelper {

    public String createScript(ScriptVariable scriptVariable, boolean isRDataTable) throws ScriptExecutionEngineException {
        String name = scriptVariable.getName();
        String value = String.valueOf(scriptVariable.getValue());
        String dirName = value.substring(0, value.indexOf(name) - 1) + TEMP + SLASH + name;
        String readType = isRDataTable ? "fread" : "read.csv";
        String assignDataTypes = String.format(ASSIGN_DATATYPES, ((AbstractDataFrame)scriptVariable.getAdditionalVariable()).getHeaderDataTypes());

        try {
            String content = IOUtils.toString((InputStream) this.getClass()
                    .getResource(CREATE_INPUT_SCRIPTS_PATH + SLASH + CREATE_R_DATA_TABLE_FILE_NAME)
                    .getContent(), StandardCharsets.UTF_8);
            content = content.replace(PATH, value)
                    .replace(READ_TYPE, readType)
                    .replace(DATAFRAME_NAME, name)
                    .replace(HEADERS, getWithHeader(isRDataTable, scriptVariable))
                    .replace(SEPARATOR, getSeparator(isRDataTable, scriptVariable))
                    .replace(FILL, getFill(isRDataTable, scriptVariable))
                    .replace(NA_STRINGS, getNaStrings(isRDataTable, scriptVariable))
                    .replace(COL_CLASSES, assignDataTypes.replace("\"", "'"));

            Path path = Paths.get(dirName);
            Files.createDirectories(path);
            FileUtils.writeStringToFile(new File(dirName + SLASH + CREATE_R_DATA_TABLE_FILE_NAME), content);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ScriptExecutionEngineException("Exception while creating read input script file for dataframe: " + name, e);
        }
        return dirName;
    }

    private String getWithHeader(boolean isRDataTable, ScriptVariable scriptVariable) {
        return isRDataTable ? Boolean.toString(((RDataTable) scriptVariable.getAdditionalVariable()).getWithHeader()).toUpperCase()
                : Boolean.toString(((RDataFrame) scriptVariable.getAdditionalVariable()).getWithHeader()).toUpperCase();
    }

    private String getSeparator(boolean isRDataTable, ScriptVariable scriptVariable) {
        return isRDataTable ? ((RDataTable) scriptVariable.getAdditionalVariable()).getSeparator()
                : ((RDataFrame) scriptVariable.getAdditionalVariable()).getSeparator();
    }

    private String getFill(boolean isRDataTable, ScriptVariable scriptVariable) {
        return isRDataTable ? (((RDataTable) scriptVariable.getAdditionalVariable()).getFill()? "T" : "F")
                : (((RDataFrame) scriptVariable.getAdditionalVariable()).getFill() ? "T" : "F");
    }

    private String getNaStrings(boolean isRDataTable, ScriptVariable scriptVariable) {
        return isRDataTable ? getNaStrings(((RDataTable) scriptVariable.getAdditionalVariable()).getNaStrings())
                : getNaStrings(((RDataFrame) scriptVariable.getAdditionalVariable()).getNaStrings());
    }

    private String getNaStrings(List<String> naStringList) {
        naStringList = naStringList.stream().map(StringEscapeUtils::escapeJava).collect(Collectors.toList());
        return isEmpty(naStringList) ? "None" : naStringList.stream().
                collect(Collectors.joining(",","'","'"));
    }
}

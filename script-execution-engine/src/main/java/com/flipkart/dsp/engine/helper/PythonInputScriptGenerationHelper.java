package com.flipkart.dsp.engine.helper;

import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.variables.PandasDataFrame;
import lombok.AllArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static com.flipkart.dsp.engine.utils.Constants.*;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

/**
 * +
 */
@AllArgsConstructor
public class PythonInputScriptGenerationHelper {

    public String createScript(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        String name = scriptVariable.getName();
        String value = String.valueOf(scriptVariable.getValue());
        String dirName = value.substring(0, value.indexOf(name) - 1) + TEMP + SLASH + name;
        String dataType = ((AbstractDataFrame) scriptVariable.getAdditionalVariable()).getHeaderDataTypes();
        PandasDataFrame pandasDataframe = (PandasDataFrame) scriptVariable.getAdditionalVariable();
        String withHeader = (pandasDataframe.getWithHeader() ? "0" : "None");

        try {
            String content = IOUtils.toString((InputStream) this.getClass()
                    .getResource(CREATE_INPUT_SCRIPTS_PATH + SLASH + CREATE_PYTHON_DATA_FRAME_FILE_NAME)
                    .getContent(), StandardCharsets.UTF_8);
            content = content.replace(PATH, value)
                    .replace(DATAFRAME_NAME, name)
                    .replace(HEADERS, withHeader)
                    .replace(SEPARATOR, pandasDataframe.getSeparator())
                    .replace(QUOTE_CHAR, pandasDataframe.getQuoteCharacter())
                    .replace(NA_VALUES, getNaValues(pandasDataframe))
                    .replace(ENCODING, pandasDataframe.getEncoding())
                    .replace(DATE_TYPE, dataType);

            Path path = Paths.get(dirName);
            Files.createDirectories(path);
            FileUtils.writeStringToFile(new File(dirName + SLASH + CREATE_PYTHON_DATA_FRAME_FILE_NAME), content);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ScriptExecutionEngineException("Exception while creating read input script file for dataframe: " + name, e);
        }
        return dirName;
    }

    private String getNaValues(PandasDataFrame pandasDataFrame) {
        List<String> naStringList = pandasDataFrame.getNaStrings();
        return isEmpty(naStringList)  ? "None" : "[" + naStringList.stream().collect(Collectors.joining(",","'","'")) + "]";
    }
}

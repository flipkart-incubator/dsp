package com.flipkart.dsp.executor.extractor;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.HashSet;
import java.util.Set;

@AllArgsConstructor
@Slf4j
public class VariableExtractor {
    private ScriptExecutionEngine scriptExecutionEngine;
    private LocationManager locationManager;

    @Timed
    @Metered
    public Set<ScriptVariable> extract(Set<ScriptVariable> outputScriptVariables) throws ScriptExecutionEngineException {
        Set<ScriptVariable> newOutputScriptVariables = new HashSet<>();
        addOutputLocationForVariables(outputScriptVariables);
        for (ScriptVariable scriptVariable : outputScriptVariables) {
            ScriptVariable var = scriptExecutionEngine.extract(scriptVariable);
            if (scriptVariable.getDataType().equals(DataType.MODEL)) {
                log.info("size of variable {} is {}", scriptVariable, FileUtils.sizeOf(new File(String.valueOf(scriptVariable.getValue()))));
            }
            if (var != null)
                newOutputScriptVariables.add(var);
        }
        return newOutputScriptVariables;
    }

    private void addOutputLocationForVariables(@NotNull Set<ScriptVariable> outputScriptVariables) {
        for (ScriptVariable scriptVariable : outputScriptVariables) {
            if (scriptVariable.getDataType().equals(DataType.DATAFRAME)
                    || scriptVariable.getDataType().equals(DataType.BYTEARRAY)
                    || scriptVariable.getDataType().equals(DataType.MODEL)) {
                String localDataFrameLocation = locationManager.getLocalFilePath(scriptVariable);
                scriptVariable.setValue(localDataFrameLocation);
            }
        }
    }

}

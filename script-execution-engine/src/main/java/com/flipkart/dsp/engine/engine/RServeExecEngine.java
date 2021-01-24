package com.flipkart.dsp.engine.engine;

import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.engine.commands.RCommands;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.engine.helper.RInputScriptGenerationHelper;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.variables.RDataFrame;
import com.flipkart.dsp.models.variables.RDataTable;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static com.flipkart.dsp.engine.utils.Constants.CREATE_R_DATA_TABLE_FILE_NAME;


/**
 */

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RServeExecEngine extends DataFrameHandler implements ScriptExecutionEngine, RCommands {

    private final RConnection connection;
    private final RInputScriptGenerationHelper RInputScriptGenerationHelper;
    private final DSPServiceConfig.ScriptExecutionConfig scriptExecutionConfig;

    @Override
    public ScriptVariable extract(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        try {
            REXP rexp;
            log.info("Extracting variable {} of type {} with value {}", scriptVariable.getName(), scriptVariable.getDataType(), scriptVariable.getValue());
            switch (scriptVariable.getDataType()) {
                case STRING:
                    rexp = run(scriptVariable.getName());
                    scriptVariable.setValue(rexp.asString());
                    break;
                case DOUBLE:
                    rexp = run(scriptVariable.getName());
                    scriptVariable.setValue(rexp.asDouble());
                    break;
                case INT:
                    rexp = run(scriptVariable.getName());
                    scriptVariable.setValue(rexp.asInteger());
                    break;
                case DATAFRAME:
                    run(IMPORT_FREAD_FWRITE);
                    run(String.format(WRITE_TO_LOCAL_FILE, scriptVariable.getName(), String.valueOf(scriptVariable.getValue()), "FALSE"));
                    break;
                case MODEL:
                case BYTEARRAY:
                    run(String.format(SAVE_RDS, scriptVariable.getName(), String.valueOf(scriptVariable.getValue())));
                    break;
                default:
                    log.error("Failed to extract Script Variable {}: Unknown datatype", scriptVariable);
                    throw new ScriptExecutionEngineException("Failed to extract Script Variable: " + scriptVariable + ". Unknown datatype");
            }
        } catch (REngineException | REXPMismatchException e) {
            log.error("Failed to extract Script Variable {}", scriptVariable, e);
            throw new ScriptExecutionEngineException("Failed to extract Script Variable: " + scriptVariable, e);
        }

        return scriptVariable;
    }

    @Override
    public void shutdown() {
        connection.close();
    }


    @Override
    public void assign(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        String name = scriptVariable.getName();
        Object value = scriptVariable.getValue();
        DataType dataType = scriptVariable.getDataType();
        log.info("Assigning variable {} of type {} with value {}", name, dataType, value);
        try {
            switch (dataType) {
                case MODEL:
                case BYTEARRAY:
                    run(String.format(READ_RDS, name, value));
                    delete(String.valueOf(value));
                    break;
                case DATAFRAME:
                    if (scriptVariable.getAdditionalVariable()==null) {
                        scriptVariable.setAdditionalVariable(new RDataFrame());
                    }
                    loadDataFrame(scriptVariable);
                    break;
                case DATAFRAME_PATH:
                case STRING:
                    connection.assign(name, String.valueOf(value));
                    break;
                case DOUBLE:
                    connection.assign(name, new double[]{(Double) value});
                    break;
                case INT:
                    connection.assign(name, new int[]{(Integer) value});
                    break;
                case DATE_TIME:
                    run(String.format(READ_DATETIME, name, value));;
                    break;
                case DATE:
                    run(String.format(READ_DATE, name, value));;
                    break;
                case LONG:
                    run(String.format(READ_NUMERIC, name, value));;
                    break;
                case BOOLEAN:
                    String rBool = "TRUE";
                    if (!(Boolean)value) rBool = "FALSE";
                    run(name + " <- " + rBool);
                    break;
                case VAR_NAME:
                    run(name + " <- " + String.valueOf(value));
                    break;
                case ARRAY:
                    assignArray(name, value);
                    break;
            }
        } catch (REngineException | REXPMismatchException| IOException | UnsupportedOperationException e) {
            throw new ScriptExecutionEngineException(String.format("Error while assigning variable: %s of type: %s", name, dataType), e);
        }
    }

    private REXP run(String cmd) throws REXPMismatchException, REngineException {
        String command = scriptExecutionConfig.isRedirectJRIConsoleToStdOut() ? "try(" + cmd + ",silent=FALSE)" : cmd;
        REXP rexp = connection.parseAndEval(command);
        if (rexp.inherits("try-error")) {
            throw new UnsupportedOperationException("Eval failed:" + rexp.asString());
        } else {
            return rexp;
        }
    }

    @Override
    public void runScript(LocalScript script) throws ScriptExecutionEngineException {
        try {
            String cmd = String.format("setwd('%s')", script.getLocation());
            run(cmd);
            String scriptName = script.getFilename();
            log.info("Running script {}", scriptName);
            run("source('" + scriptName + "')");
        } catch (REXPMismatchException | REngineException | UnsupportedOperationException e) {
            log.error("Failed to run script {}", script, e);
            throw new ScriptExecutionEngineException("Failed to run script : "+ script.toString(), e);
        }
    }

    private void delete(String path) throws IOException {
        if (scriptExecutionConfig.isDebugMode()) {
            return;
        }
        Files.delete(Paths.get(path));
    }

    private void assignArray(String name, Object value) throws REngineException {
        Object[] array = value instanceof List ? (((List) value).toArray()) : (Object[]) value;
        if(array.length == 0) return;
        if (array[0] instanceof Integer) {
            int[] primitiveArray = new int[array.length];
            for (int i = 0; i < array.length; i++) {
                primitiveArray[i] = (Integer) array[i];
            }
            connection.assign(name, primitiveArray);
        } else if (array[0] instanceof Double) {
            double[] primitiveArray = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                primitiveArray[i] = (Double) array[i];
            }
            connection.assign(name, primitiveArray);
        } else if (array[0] instanceof String) {
            String[] primitiveArray = new String[array.length];
            for (int i = 0; i < array.length; i++) {
                primitiveArray[i] = (String) array[i];
            }
            log.info("Assignment= " + "name: " + name + " , " + "value" + " : " + Arrays.toString(primitiveArray));
            connection.assign(name, primitiveArray);
        } else {
            throw new UnsupportedOperationException("Unable to convert Object to int/double/String array: " + value);
        }
    }

    @Override
    public void loadOnly(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        readAbstractDataFrame(scriptVariable);
        assignHeaderNames(scriptVariable);
    }

    @Override
    public void deleteDataFrame(String path) throws ScriptExecutionEngineException {
        try {
            run(String.format(DELETE_FOLDER, path));
        } catch (REXPMismatchException | REngineException e) {
            throw new ScriptExecutionEngineException("Failed to delete file from path: " + path, e);
        }
    }

    private void readAbstractDataFrame(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        boolean isRDataTable = scriptVariable.getAdditionalVariable() instanceof  RDataTable;

        try {
            String location = RInputScriptGenerationHelper.createScript(scriptVariable, isRDataTable);
            runScript(LocalScript.builder().location(location).filename(CREATE_R_DATA_TABLE_FILE_NAME).build());
        } catch (Exception e) {
            e.printStackTrace();
            throw new ScriptExecutionEngineException("cannot read " + scriptVariable.toString() + " into DATAFRAME");
        }
    }

    private void assignHeaderNames(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        String name = scriptVariable.getName();
        log.info("Column name and datatype " + ((AbstractDataFrame)scriptVariable.getAdditionalVariable()).getHeaderDataTypes());
        String command = String.format(ASSIGN_HEADERS, name, ((AbstractDataFrame)scriptVariable.getAdditionalVariable()).getHeaders());
        try {
            run(command);
        } catch (Exception e) {
            throw new ScriptExecutionEngineException("Failed to assign header for: " + name, e);
        }
    }

}

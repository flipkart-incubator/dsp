package com.flipkart.dsp.engine.engine;


import com.flipkart.dsp.engine.commands.PyCommands;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.engine.helper.PythonInputScriptGenerationHelper;
import com.flipkart.dsp.engine.thrift.ScriptExecutionEngineImpl;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.variables.PandasDataFrame;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.List;

import static com.flipkart.dsp.engine.utils.Constants.CREATE_PYTHON_DATA_FRAME_FILE_NAME;


@Slf4j
@RequiredArgsConstructor
public class PyExecEngine extends DataFrameHandler implements ScriptExecutionEngine, PyCommands {
    private final ScriptExecutionEngineImpl.Client client;
    private final PythonInputScriptGenerationHelper pythonInputScriptGenerationHelper;

    @Override
    public void shutdown() {
        try {
            client.shutdown();
        } catch (TException e) {
            log.warn("Could not shutdown PyExecEngine");
        }
    }

    @Override
    public void assign(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        String name = scriptVariable.getName();
        Object value = scriptVariable.getValue();
        log.info("Assigning variable {} of type {} with value {}", scriptVariable.getName(), scriptVariable.getDataType(), scriptVariable.getValue());
        try {
            switch (scriptVariable.getDataType()) {
                case MODEL:
                case BYTEARRAY:
                    client.runCommand(String.format(READ_MODEL, name, value));
                    break;
                case DATAFRAME:
                    if (scriptVariable.getAdditionalVariable() == null) {
                        scriptVariable.setAdditionalVariable(new PandasDataFrame());
                    }
                    loadDataFrame(scriptVariable);
                    break;
                case DATAFRAME_PATH:
                case STRING:
                    client.runCommand(String.format(ASSIGN, name) + "'" + value + "'");
                    break;
                case BOOLEAN:
                    String pyBool = "True";
                    if (!(Boolean.valueOf(String.valueOf(value)))) pyBool = "False";
                    client.runCommand(String.format(ASSIGN, name) + pyBool);
                    break;
                case DOUBLE:
                    client.runCommand(String.format(IMPORT_DECIMAL, name) + "Decimal('" + value + "')");
                    break;
                case DATE_TIME:
                    client.runCommand(String.format(IMPORT_DATETIME, name) + "datetime.strptime('" + value + "', '%Y-%m-%dT%H:%M:%S')");
                    break;
                case DATE:
                    client.runCommand(String.format(IMPORT_DATETIME, name) + "datetime.strptime('" + value + "', '%Y-%m-%d').date()");
                    break;
                case LONG:
                case INT:
                    client.runCommand(String.format(ASSIGN, name) + "int('" + value + "')");
                    break;
                case ARRAY:
                    assignArray(name, value);
                    break;
            }
        } catch (TException e) {
            throw new ScriptExecutionEngineException(String.format("Error while assigning variable: %s of type: %s with value %s",
                    scriptVariable.getName(), scriptVariable.getDataType(), scriptVariable.getValue()), e);
        }
    }

    @Override
    public void runScript(LocalScript script) throws ScriptExecutionEngineException {
        try {
            runCommand(String.format(SET_WD, script.getLocation()));
            client.runScript(script.getFilename());
        } catch (TException e) {
            e.printStackTrace();
            throw new ScriptExecutionEngineException("Failed to run script : " + script.toString(), e);
        }
    }

    @Override
    public ScriptVariable extract(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        log.info("Extracting variable {} of type {} with value {}", scriptVariable.getName(), scriptVariable.getDataType(), scriptVariable.getValue());
        String name = scriptVariable.getName();
        Object value = scriptVariable.getValue();
        DataType dataType = scriptVariable.getDataType();
        switch (dataType) {
            case STRING:
                scriptVariable.setValue(evalCommand(name));
                break;
            case DOUBLE:
                scriptVariable.setValue(Double.valueOf(evalCommand(name)));
                break;
            case INT:
                scriptVariable.setValue(Integer.valueOf(evalCommand(name)));
                break;
            case DATAFRAME:
                PandasDataFrame pandasDataFrame = scriptVariable.getAdditionalVariable() == null
                        ? new PandasDataFrame() : (PandasDataFrame) scriptVariable.getAdditionalVariable();
                runCommand(String.format(WRITE_TO_LOCAL_FILE, name, value, pandasDataFrame.getSeparator(),
                        pandasDataFrame.getEncoding(), pandasDataFrame.getQuoteCharacter()));
                break;
            case MODEL:
            case BYTEARRAY:
                runCommand(String.format(SAVE_MODEL, name, String.valueOf(value)));
                break;
            default:
                log.error("Failed to extract Script Variable {}: Unknown DataType", scriptVariable);
                throw new ScriptExecutionEngineException("Failed to extract Script Variable: " + scriptVariable + ". Unknown DataType");
        }
        return scriptVariable;
    }

    private void runCommand(String command) throws ScriptExecutionEngineException {
        try {
            client.runCommand(command);
        } catch (TException e) {
            throw new ScriptExecutionEngineException("Failed to run command : " + command, e);
        }
    }

    private String evalCommand(String command) throws ScriptExecutionEngineException {
        try {
            return client.evalCommand(command);
        } catch (TException e) {
            throw new ScriptExecutionEngineException("Failed to eval command :" + command, e);
        }
    }

    private void assignArray(String name, Object value) throws TException {
        Object[] array = value instanceof List ? (((List) value).toArray()) : (Object[]) value;
        if (array.length > 0) {
            if (array[0] instanceof Integer) {
                int[] primitiveArray = new int[array.length];
                for (int i = 0; i < array.length; i++) {
                    primitiveArray[i] = (Integer) array[i];
                }
                client.runCommand(String.format(ASSIGN, name) + Arrays.toString(primitiveArray));
            } else if (array[0] instanceof Double) {
                double[] primitiveArray = new double[array.length];
                for (int i = 0; i < array.length; i++) {
                    primitiveArray[i] = (Double) array[i];
                }
                client.runCommand(String.format(ASSIGN, name) + Arrays.toString(primitiveArray));
            } else if (array[0] instanceof String) {
                String[] primitiveArray = new String[array.length];
                for (int i = 0; i < array.length; i++) {
                    primitiveArray[i] = (String) array[i];
                }
                String join = "['" +
                        StringUtils.join(primitiveArray, "', '") +
                        "']";
                client.runCommand(String.format(ASSIGN, name) + join);
            }
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
            client.runCommand(String.format(DELETE_FOLDER, path));
        } catch (TException e) {
            throw new ScriptExecutionEngineException("Failed to delete folder from path: " + path, e);
        }
    }

    private void readAbstractDataFrame(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        try {
            String location = pythonInputScriptGenerationHelper.createScript(scriptVariable);
            runScript(LocalScript.builder().location(location).filename(CREATE_PYTHON_DATA_FRAME_FILE_NAME).build());
        } catch (Exception e) {
            e.printStackTrace();
            throw new ScriptExecutionEngineException("Failed to read dataFrame: " + scriptVariable.getName(), e);
        }
    }

    private void assignHeaderNames(ScriptVariable scriptVariable) throws ScriptExecutionEngineException {
        String name = scriptVariable.getName();
        try {
            String command = String.format(ASSIGN_HEADER, name, ((AbstractDataFrame) scriptVariable.getAdditionalVariable()).getHeaders());
            log.info("running command to set header => " + command);
            client.runCommand(command);
            log.info("Column name and datatype " + ((AbstractDataFrame) scriptVariable.getAdditionalVariable()).getHeaderDataTypes());
        } catch (Exception e) {
            throw new ScriptExecutionEngineException("Failed to assign header for: " + name, e);
        }
    }

}

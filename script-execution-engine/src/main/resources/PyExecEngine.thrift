namespace java com.flipkart.dsp.engine.thrift

enum AdditionalVariableInfoDTO {
DATAFRAME_DELIMITER,
HIVE_TABLE
}

enum DataTypeDTO {
STRING = 1,
INT = 2,
DOUBLE = 3,
BOOLEAN = 4
}

struct ScriptVariableDTO {
1:string name;
2:DataTypeDTO dataType;
3:string value;
4:map<AdditionalVariableInfoDTO,string> additionalParams;
}

exception ScriptExecutionEngineExceptionDTO {
1:string message
}


service ScriptExecutionEngineImpl {
    void shutdown(),
    void assign(1: ScriptVariableDTO scriptVariable) throws (1:ScriptExecutionEngineExceptionDTO message),
    void runScript(1: string scriptName) throws (1:ScriptExecutionEngineExceptionDTO messgae),
    ScriptVariableDTO extract(1: ScriptVariableDTO scriptVariable) throws (1:ScriptExecutionEngineExceptionDTO messgae),
    void runCommand(1: string command) throws (1:ScriptExecutionEngineExceptionDTO messgae),
    string evalCommand(1: string command) throws (1:ScriptExecutionEngineExceptionDTO messgae);
}

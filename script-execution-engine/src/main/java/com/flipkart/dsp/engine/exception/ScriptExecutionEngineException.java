package com.flipkart.dsp.engine.exception;

public class ScriptExecutionEngineException extends Exception {
    public ScriptExecutionEngineException(String message, Throwable cause) {
        super("Script Execution Engine failed because of following reason: " + message, cause);
    }

    public ScriptExecutionEngineException(String message) {
        super(message);
    }

    public ScriptExecutionEngineException(Throwable cause) {
        super(cause);
    }
}

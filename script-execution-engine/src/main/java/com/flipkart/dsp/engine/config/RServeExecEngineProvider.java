package com.flipkart.dsp.engine.config;

import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.engine.helper.RInputScriptGenerationHelper;
import com.flipkart.dsp.engine.engine.RServeExecEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;

import javax.inject.Inject;
import javax.inject.Provider;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RServeExecEngineProvider implements Provider<RServeExecEngine> {

    private final RInputScriptGenerationHelper RInputScriptGenerationHelper;
    private final DSPServiceConfig.ScriptExecutionConfig scriptExecutionConfig;

    @Override
    public RServeExecEngine get() {
        try {
            return new RServeExecEngine(provideRConnection(), RInputScriptGenerationHelper, scriptExecutionConfig);
        } catch (REngineException | REXPMismatchException e) {
            throw new RuntimeException(e);
        }
    }

    RConnection provideRConnection() throws REngineException, REXPMismatchException {
        RConnection connection;
        connection = new RConnection();
        String cmd = String.format("setwd('%s')", scriptExecutionConfig.getWorkingDir());
        connection.parseAndEval(cmd);
        log.info("RConnection established");
        return connection;
    }
}

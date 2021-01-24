package com.flipkart.dsp.engine.engine;

import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.engine.commands.RCommands;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.models.ScriptVariable;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rosuda.REngine.JRI.JRIEngine;
import org.rosuda.REngine.*;

import java.lang.reflect.Field;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RExecutionEngine implements ScriptExecutionEngine,RCommands {

    private static REngine rEngine;
    private static boolean initialized;
    private final DSPServiceConfig.ScriptExecutionConfig scriptExecutionConfig;


    public ScriptVariable extract(ScriptVariable scriptVariable) {
        //todo: implement when needed
        return null;
    }


    @Override
    public void shutdown() {
        rEngine.close();
        initialized = false;
    }

    @Override
    public void assign(ScriptVariable scriptVariable) {

    }

    public boolean initREngine() {
        try {
            if (!initialized) {
                linkRJava();
                REngineStdOutput redirectedOutput = scriptExecutionConfig.isRedirectJRIConsoleToStdOut() ? new REngineStdOutput() : null;
                rEngine = new JRIEngine(new String[]{"--no-save"}, redirectedOutput, false);
                String cmd = String.format("setwd('%s')", scriptExecutionConfig.getWorkingDir());
                rEngine.parseAndEval(cmd);
                log.info("REngine Initialized");
                initialized = true;
            }
        } catch (REngineException | REXPMismatchException e) {
            log.error("Error while initializing REngine. Options- Redirect console: {}, Working dir: {}",
                    scriptExecutionConfig.isRedirectJRIConsoleToStdOut(), scriptExecutionConfig.getWorkingDir(), e);
            initialized = false;
        }
        return initialized;
    }

    private void linkRJava() {
        //Can be skipped when moving to storm as JRI lib path will already be set
        try {
            System.setProperty("java.library.path", scriptExecutionConfig.getRLibraryPath());
            Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
            fieldSysPath.setAccessible(true);
            fieldSysPath.set(null, null);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Could not link rJava", e);
        }
    }

    public REXP run(String cmd) throws REXPMismatchException, REngineException {
        return rEngine.parseAndEval(cmd);
    }

    public void runScript(LocalScript script) throws ScriptExecutionEngineException {
        try {
            String cmd = String.format("setwd('%s')", script.getLocation());
            run(cmd);
            String scriptName = script.getFilename();
            log.debug("Running script {}", scriptName);
            run("source('" + scriptName + "')");
        } catch (REXPMismatchException e) {
            e.printStackTrace();
            throw new ScriptExecutionEngineException("");
        } catch (REngineException e) {
            e.printStackTrace();
            throw new ScriptExecutionEngineException("");
        }
    }

}

package com.flipkart.dsp.executor.runner;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.executor.extractor.VariableExtractor;
import com.flipkart.dsp.executor.loader.VariableLoader;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.ImageLanguageEnum;
import com.flipkart.dsp.models.ScriptVariable;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Provider;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class ScriptRunner {
    private final LocationManager locationManager;
    private ScriptExecutionEngine scriptExecutionEngine;
    private final Map<ImageLanguageEnum, Provider<ScriptExecutionEngine>> scriptExecutionEngineMap;

    @Timed
    public Set<ScriptVariable> run(LocalScript script, Set<ScriptVariable> scriptVariableSet) throws ScriptExecutionEngineException {
        scriptExecutionEngine = scriptExecutionEngineMap.get(script.getImageLanguageEnum()).get();
        VariableLoader variableLoader = new VariableLoader(scriptExecutionEngine);
        VariableExtractor variableExtractor = new VariableExtractor(scriptExecutionEngine, locationManager);
        variableLoader.load(scriptVariableSet);
        scriptExecutionEngine.runScript(script);
        return variableExtractor.extract(script.getOutputVariables());
    }

    @Timed
    public Set<ScriptVariable> run(LocalScript script) throws ScriptExecutionEngineException {
        scriptExecutionEngine = scriptExecutionEngineMap.get(script.getImageLanguageEnum()).get();
        scriptExecutionEngine.runScript(script);
        return script.getOutputVariables();
    }
}

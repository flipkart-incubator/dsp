package com.flipkart.dsp.engine.engine;

import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.models.ScriptVariable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
@Slf4j
@RequiredArgsConstructor
public class BashExecutionEngine implements ScriptExecutionEngine {

    private final ProcessBuilder processBuilder;

    public void shutdown() {
    }

    public void assign(ScriptVariable scriptVariable) {
    }


    public void runScript(LocalScript script) throws ScriptExecutionEngineException {
        try {
            processBuilder.command(generateCommand(script));
            Process process = processBuilder.start();
            StringBuilder output = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line);
            }

            int exitVal = process.waitFor();

            if (exitVal != 0)
                throw new ScriptExecutionEngineException("Error in running script for get ImageDetails");

            setOutputVariables(script, output);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            throw new ScriptExecutionEngineException(e.getMessage());
        }
    }

    private void setOutputVariables(LocalScript script, StringBuilder output) {
        Set<ScriptVariable> outputVariables = new HashSet<>();
        outputVariables.add(ScriptVariable.builder().value(output.toString()).build());
        script.setOutputVariables(outputVariables);
    }

    private List<String> generateCommand(LocalScript script) {
        List<String> command  = new ArrayList<>();
        command.add(script.getLocation());
        command.addAll((List)script.getInputVariables().stream().map(scriptVariable -> scriptVariable.getValue()).collect(Collectors.toList()));
        return command;
    }

    public ScriptVariable extract(ScriptVariable scriptVariable) {
        return null;
    }
}

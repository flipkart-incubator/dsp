package com.flipkart.dsp.engine.config;

import com.flipkart.dsp.engine.engine.BashExecutionEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class BashExecEngineProvider implements Provider<BashExecutionEngine> {

    private final ProcessBuilder processBuilder;

    @Override
    public BashExecutionEngine get() {
        return new BashExecutionEngine(processBuilder);
    }

}

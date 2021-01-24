package com.flipkart.dsp.engine.config;

import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.models.ImageLanguageEnum;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;

public class ScriptExecutionModule extends AbstractModule {
    @Override
    protected void configure() {
        MapBinder<ImageLanguageEnum, ScriptExecutionEngine> mapbinder =
                MapBinder.newMapBinder(binder(), ImageLanguageEnum.class, ScriptExecutionEngine.class);
        mapbinder.addBinding(ImageLanguageEnum.PYTHON2).toProvider(PythonExecEngineProvider.class);
        mapbinder.addBinding(ImageLanguageEnum.PYTHON3).toProvider(PythonExecEngineProvider.class);
        mapbinder.addBinding(ImageLanguageEnum.R).toProvider(RServeExecEngineProvider.class);
        mapbinder.addBinding(ImageLanguageEnum.BASH).toProvider(BashExecEngineProvider.class);
    }

    @Provides
    @Singleton
    public ProcessBuilder provideProcessBuilder() {
        return new ProcessBuilder();
    }

}

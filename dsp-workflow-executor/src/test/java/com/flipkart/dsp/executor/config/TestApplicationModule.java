package com.flipkart.dsp.executor.config;

import com.flipkart.dsp.engine.engine.BashExecutionEngine;
import com.flipkart.dsp.engine.engine.ScriptExecutionEngine;
import com.flipkart.dsp.executor.utils.LocationHelper;
import com.flipkart.dsp.executor.utils.TestLocationHelper;
import com.flipkart.dsp.executor.utils.TestScriptExecEngine;
import com.flipkart.dsp.models.ImageLanguageEnum;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;

public class TestApplicationModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocationHelper.class).to(TestLocationHelper.class);

        MapBinder<ImageLanguageEnum,ScriptExecutionEngine> binder = MapBinder.newMapBinder(binder(),
                ImageLanguageEnum.class, ScriptExecutionEngine.class);
        binder.addBinding(ImageLanguageEnum.PYTHON2).toInstance(new TestScriptExecEngine());
        binder.addBinding(ImageLanguageEnum.PYTHON3).toInstance(new TestScriptExecEngine());
        binder.addBinding(ImageLanguageEnum.R).toInstance(new TestScriptExecEngine());
        binder.addBinding(ImageLanguageEnum.BASH).toInstance(new BashExecutionEngine(new ProcessBuilder()));
    }
}

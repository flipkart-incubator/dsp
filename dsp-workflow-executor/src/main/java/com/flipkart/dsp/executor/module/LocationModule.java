package com.flipkart.dsp.executor.module;

import com.flipkart.dsp.executor.utils.LocationHelper;
import com.flipkart.dsp.executor.utils.MesosLocationHelper;
import com.google.inject.AbstractModule;

public class LocationModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocationHelper.class).to(MesosLocationHelper.class);
    }
}

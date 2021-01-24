package com.flipkart.dsp.utils;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

/**
 */
public class MetricsRegistryHelper {

    private final MetricsRegistry registry;

    @Inject
    public MetricsRegistryHelper(MetricsRegistry registry) {
        this.registry = registry;
    }
    public Meter getMeterInstance(Class className, String fieldType) {
        return registry.newMeter(className, fieldType, "dspService", fieldType, TimeUnit.SECONDS);
    }
    public Histogram getHistogramInstance(Class className, String fieldType) {
        return registry.newHistogram(className, fieldType, "dspService");
    }
}

package com.flipkart.dsp.cosmos;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class CosmosReporter extends ScheduledReporter {
    private static final String SPACE=" ";
    private static final String CURRENT_MEMORY_USAGE_FILE_PATH = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
    private static final String MAX_MEMORY_FILE_PATH = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
    private static Logger LOGGER;
    private static String LOGGER_FORMAT = "%d %s.%s %s%s";
    private static Object COSMOSTAG;
    private final MetricRegistry registry;
    private static boolean DOCKER_MEMORY_OPTION;

    public static CosmosReporter create(MetricRegistry registry,
                                        Logger logger,
                                        Object cosmosTag,
                                        TimeUnit metricsUnits,
                                        Boolean dockerMemoryOption) {
        LOGGER = logger;
        COSMOSTAG = cosmosTag;
        DOCKER_MEMORY_OPTION = dockerMemoryOption;
        return new CosmosReporter(registry, "cosmos-dsp-metrics", MetricFilter.ALL, TimeUnit.SECONDS, metricsUnits);
    }

    protected CosmosReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.registry = registry;
    }

    @Override
    public void report(SortedMap<String, Gauge> gaugeMap, SortedMap<String, Counter> counterMap, SortedMap<String, Histogram> histogramMap, SortedMap<String, Meter> meterMap, SortedMap<String, Timer> timerMap) {
        logForMemory();
        logForTimer(timerMap);
        logForMeter(meterMap);
    }

    private void logForMeter(SortedMap<String, Meter> meterMap) {
        if (meterMap.isEmpty()) return;
        Iterator var6 = meterMap.entrySet().iterator();
        while (var6.hasNext()) {
            Map.Entry entry = (Map.Entry) var6.next();
            this.logMeter((String) entry.getKey(), (Meter) entry.getValue());
        }
    }

    protected void logForMemory() {
        if(DOCKER_MEMORY_OPTION && new File(MAX_MEMORY_FILE_PATH).exists()) {
            try {
                long currentTime = Instant.now().toEpochMilli();
                String cosmosTag = getCosmosAttributes();
                String memoryUsage = FileUtils.readFileToString(new File(CURRENT_MEMORY_USAGE_FILE_PATH), "UTF-8");
                String memoryMax = FileUtils.readFileToString(new File(MAX_MEMORY_FILE_PATH), "UTF-8");
                LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, "Memory", "max", memoryMax.trim(), cosmosTag));
                LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, "Memory", "usage", memoryUsage.trim(), cosmosTag));

            } catch (IOException e) {
                LOGGER.info("Issue while reading memory limit from file");
            }
        }
    }

    protected void logMeter(String name, Meter meter) {
        long currentTime = Instant.now().toEpochMilli();
        String cosmosTag = getCosmosAttributes();
        LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, name, "CountStarted", meter.getCount(), cosmosTag));
        LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, name, "MeanRate", meter.getMeanRate(), cosmosTag));
    }

    private void logForTimer(Map<String, Timer> timerMap) {
        if (timerMap.isEmpty()) return;
        Iterator var6 = timerMap.entrySet().iterator();
        while (var6.hasNext()) {
            Map.Entry entry = (Map.Entry) var6.next();
            this.logTimer((String) entry.getKey(), (Timer) entry.getValue());
        }
    }

    protected void logTimer(String name, Timer timer) {
        long currentTime = Instant.now().toEpochMilli();
        Snapshot snapshot = timer.getSnapshot();
        String cosmosTag = getCosmosAttributes();
        LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, name, "CountCompleted", timer.getCount(), cosmosTag));
        LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, name, "FifteenMinuteRate", timer.getFifteenMinuteRate(), cosmosTag));
        LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, name, "FiveMinuteRate", timer.getFiveMinuteRate(), cosmosTag));
        LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, name, "min", this.convertDuration((double) snapshot.getMin()), cosmosTag));
        LOGGER.info(String.format(LOGGER_FORMAT, currentTime / 1000L, name, "max", this.convertDuration((double) snapshot.getMax()), cosmosTag));
    }

    public void forceFlush() {
        logForMemory();
        logForMeter(registry.getMeters());
        logForTimer(registry.getTimers());
    }

    protected String getCosmosAttributes() {
        StringBuffer stringBuffer = new StringBuffer(SPACE);
        Field[] fields = COSMOSTAG.getClass().getDeclaredFields();
        Arrays.stream(fields).forEach(field -> {
            field.setAccessible(true);
            try {
                String value;
                if(field.get(COSMOSTAG) == null) {
                    value = null;
                } else {
                    value = field.get(COSMOSTAG).toString().replaceAll("[^a-zA-Z0-9.-]", "_");
                }
                stringBuffer.append(String.format("%s=%s",field.getName(), value));
                stringBuffer.append(SPACE);

            } catch (final IllegalAccessException e) {

            }
        });
        return stringBuffer.toString();
    }
}

package io.github.ooohyg.distributed.cache.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MicrometerCacheMetrics implements CacheMetrics {

    private final MeterRegistry registry;
    private final ConcurrentMap<String, Counter> counterCache = new ConcurrentHashMap<>();

    public MicrometerCacheMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    private Counter getCounter(String name, String level) {
        String key = name + ":" + level;
        return counterCache.computeIfAbsent(key, k ->
                Counter.builder("cache." + name).tag("level", level).register(registry));
    }

    @Override
    public void recordHit(String level) {
        getCounter("hit", level).increment();
    }

    @Override
    public void recordMiss(String level) {
        getCounter("miss", level).increment();
    }

    @Override
    public void recordNullHit(String level) {
        getCounter("null_hit", level).increment();
    }

    @Override
    public void recordLoadTime(Duration duration) {
        Timer.builder("cache.load.time").register(registry).record(duration);
    }

    @Override
    public void recordLoadFailure(Throwable error) {
        getCounter("load_failure", "none").increment();
    }

    @Override
    public void recordSingleFlightMerge() {
        getCounter("singleflight_merge", "none").increment();
    }

    @Override
    public void recordInvalidation() {
        getCounter("invalidation_success", "none").increment();
    }

    @Override
    public void recordStaleEventIgnored() {
        getCounter("invalidation_stale", "none").increment();
    }
}
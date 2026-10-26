package io.github.ooohyg.distributed.cache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.ooohyg.distributed.cache.Cache;
import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.EventType;
import io.github.ooohyg.distributed.cache.VersionedValue;
import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import io.github.ooohyg.distributed.cache.api.CacheLevel;
import io.github.ooohyg.distributed.cache.api.ReadContext;
import io.github.ooohyg.distributed.cache.api.SourceReader;
import io.github.ooohyg.distributed.cache.api.SourceWriter;
import io.github.ooohyg.distributed.cache.core.strategy.read.ReadStrategy;
import io.github.ooohyg.distributed.cache.core.strategy.write.WriteContext;
import io.github.ooohyg.distributed.cache.core.strategy.write.WriteStrategy;
import io.github.ooohyg.distributed.cache.failure.FailurePolicy;
import io.github.ooohyg.distributed.cache.failure.NoOpFailurePolicy;
import io.github.ooohyg.distributed.cache.invalidation.CacheInvalidationBus;
import io.github.ooohyg.distributed.cache.invalidation.CacheInvalidationPublisher;
import io.github.ooohyg.distributed.cache.invalidation.CacheInvalidationSubscriber;
import io.github.ooohyg.distributed.cache.metrics.CacheMetrics;
import io.github.ooohyg.distributed.cache.protection.CacheAvalancheGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * 多级缓存编排器。
 */
public class MultiLevelCache<K, V> implements Cache<K, V>, CacheInvalidationSubscriber {

    private static final Logger log = LoggerFactory.getLogger(MultiLevelCache.class);

    private static final Duration EVENT_ID_TTL = Duration.ofMinutes(5);
    private static final long EVENT_ID_MAX_SIZE = 1_000_000L;
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(30);
    private static final Duration DEFAULT_NULL_VALUE_TTL = Duration.ofMinutes(1);

    private final CacheLevel<K, V> l1;
    private final CacheLevel<K, V> l2;
    private final SourceReader<K, V> reader;
    private final SourceWriter<K, V> writer;
    private final ReadStrategy<K, V> readStrategy;
    private final WriteStrategy<K, V> writeStrategy;
    private final CacheInvalidationPublisher publisher;
    private final CacheMetrics metrics;
    private final Duration configuredDefaultTtl;
    private final CacheAvalancheGuard<K> avalancheGuard;
    private final FailurePolicy<K, V> failurePolicy;
    private final Duration nullValueTtl;

    private final Function<K, String> keyToString;
    private final Function<String, K> keyFromString;

    private final com.github.benmanes.caffeine.cache.Cache<String, String> seenEvents =
            Caffeine.newBuilder()
                    .expireAfterWrite(EVENT_ID_TTL)
                    .maximumSize(EVENT_ID_MAX_SIZE)
                    .build();

    /** 简化构造：默认 TTL、无防雪崩、不降级、空值 TTL 1 分钟。 */
    @SuppressWarnings("unchecked")
    public MultiLevelCache(CacheLevel<K, V> l1,
                           CacheLevel<K, V> l2,
                           SourceReader<K, V> reader,
                           SourceWriter<K, V> writer,
                           ReadStrategy<K, V> readStrategy,
                           WriteStrategy<K, V> writeStrategy,
                           CacheInvalidationPublisher publisher,
                           CacheMetrics metrics) {
        this(l1, l2, reader, writer, readStrategy, writeStrategy, publisher, metrics,
                DEFAULT_TTL, null, new NoOpFailurePolicy<>(),
                DEFAULT_NULL_VALUE_TTL,
                String::valueOf, s -> (K) s);
    }

    /** 完整构造。 */
    public MultiLevelCache(CacheLevel<K, V> l1,
                           CacheLevel<K, V> l2,
                           SourceReader<K, V> reader,
                           SourceWriter<K, V> writer,
                           ReadStrategy<K, V> readStrategy,
                           WriteStrategy<K, V> writeStrategy,
                           CacheInvalidationPublisher publisher,
                           CacheMetrics metrics,
                           Duration configuredDefaultTtl,
                           CacheAvalancheGuard<K> avalancheGuard,
                           FailurePolicy<K, V> failurePolicy,
                           Duration nullValueTtl,
                           Function<K, String> keyToString,
                           Function<String, K> keyFromString) {
        this.l1 = l1;
        this.l2 = l2;
        this.reader = reader;
        this.writer = writer;
        this.readStrategy = readStrategy;
        this.writeStrategy = writeStrategy;
        this.publisher = publisher;
        this.metrics = metrics;
        this.configuredDefaultTtl = configuredDefaultTtl;
        this.avalancheGuard = avalancheGuard;
        this.failurePolicy = failurePolicy;
        this.nullValueTtl = nullValueTtl;
        this.keyToString = keyToString;
        this.keyFromString = keyFromString;
        if (publisher instanceof CacheInvalidationBus) {
            ((CacheInvalidationBus) publisher).subscribe(this);
        }
    }

    @Override
    public Optional<V> get(K key) {
        Optional<CacheEntry<V>> entry = readStrategy.read(key, new DefaultReadContext(key));
        if (entry.isEmpty()) {
            return Optional.empty();
        }
        CacheEntry<V> e = entry.get();
        if (e.nullValue()) {
            // 命中空值占位符——不穿透，也不返回值
            metrics.recordNullHit("cache");
            return Optional.empty();
        }
        return Optional.of(e.value());
    }

    @Override
    public void put(K key, V value) {
        put(key, value, configuredDefaultTtl);
    }

    @Override
    public void put(K key, V value, Duration ttl) {
        writeStrategy.put(key, value, new DefaultWriteContext(key, ttl));
    }

    @Override
    public void invalidate(K key) {
        writeStrategy.invalidate(key, new DefaultWriteContext(key, configuredDefaultTtl));
    }

    @Override
    public void invalidateAll() {
        l1.clear();
        l2.clear();
    }

    @Override
    public void close() {
        l1.clear();
        l2.clear();
    }

    @Override
    public void onEvent(CacheInvalidationEvent event) {
        String fingerprint = event.key() + "|" + event.version() + "|" + event.eventType();
        String existingFingerprint = seenEvents.getIfPresent(event.eventId());
        if (existingFingerprint != null) {
            if (existingFingerprint.equals(fingerprint)) {
                return;
            } else {
                log.warn("eventId collision detected: eventId={}, existingFingerprint={}, incomingFingerprint={}",
                        event.eventId(), existingFingerprint, fingerprint);
                return;
            }
        }
        seenEvents.put(event.eventId(), fingerprint);

        K key = keyFromString.apply(event.key());
        Optional<CacheEntry<V>> current = l1.getEntry(key);
        if (event.version() >= 0 && current.isPresent()
                && current.get().version() > event.version()) {
            metrics.recordStaleEventIgnored();
            return;
        }
        l1.evict(key);
        l2.evict(key);
        metrics.recordInvalidation();
    }

    public CacheMetrics metrics() {
        return metrics;
    }

    // ---------------- 读上下文 ----------------

    private final class DefaultReadContext implements ReadContext<K, V> {

        private final K key;

        DefaultReadContext(K key) {
            this.key = key;
        }

        @Override
        public Duration defaultTtl() {
            if (avalancheGuard == null) {
                return configuredDefaultTtl;
            }
            return avalancheGuard.computeTtl(key, configuredDefaultTtl);
        }

        @Override
        public Duration nullValueTtl() {
            return nullValueTtl;
        }

        @Override
        public Optional<CacheEntry<V>> readL1(K key) {
            Optional<CacheEntry<V>> entry = l1.getEntry(key);
            if (entry.isPresent() && entry.get().isPhysicalExpired(System.currentTimeMillis())) {
                l1.evict(key);
                return Optional.empty();
            }
            return entry;
        }

        @Override
        public Optional<CacheEntry<V>> readL2(K key) {
            Optional<CacheEntry<V>> entry = l2.getEntry(key);
            if (entry.isPresent() && entry.get().isPhysicalExpired(System.currentTimeMillis())) {
                l2.evict(key);
                return Optional.empty();
            }
            return entry;
        }

        @Override
        public Optional<CacheEntry<V>> peekStale(K key) {
            Optional<CacheEntry<V>> l1Entry = l1.getEntry(key);
            if (l1Entry.isPresent()) {
                return l1Entry;
            }
            return l2.getEntry(key);
        }

        @Override
        public void writeL1(K key, CacheEntry<V> entry) {
            writeIfNewer(l1, key, entry);
        }

        @Override
        public void writeL2(K key, CacheEntry<V> entry) {
            writeIfNewer(l2, key, entry);
        }

        @Override
        public VersionedValue<V> loadFromSource(K key) throws Exception {
            return reader.read(key);
        }

        @Override
        public Optional<CacheEntry<V>> handleLoadFailure(K key, Throwable error) {
            Optional<CacheEntry<V>> stale = peekStale(key);
            Optional<V> fallback = failurePolicy.onLoadFailure(key, error);
            if (fallback.isPresent()) {
                long version = stale.map(CacheEntry::version).orElse(0L);
                VersionedValue<V> wrapped = new VersionedValue<>(fallback.get(), version);
                return Optional.of(CacheEntry.from(wrapped, configuredDefaultTtl));
            }
            return stale;
        }

        @Override
        public void recordHit(String level) {
            metrics.recordHit(level);
        }

        @Override
        public void recordMiss(String level) {
            metrics.recordMiss(level);
        }

        @Override
        public void recordLoadTime(Duration duration) {
            metrics.recordLoadTime(duration);
        }
    }

    // ---------------- 写上下文 ----------------

    private final class DefaultWriteContext implements WriteContext<K, V> {

        private final Duration ttl;

        DefaultWriteContext(K key, Duration ttl) {
            this.ttl = ttl;
        }

        @Override
        public Duration ttl() {
            return ttl;
        }

        @Override
        public void writeL1(K key, CacheEntry<V> entry) {
            writeIfNewer(l1, key, entry);
        }

        @Override
        public void writeL2(K key, CacheEntry<V> entry) {
            writeIfNewer(l2, key, entry);
        }

        @Override
        public void invalidateL1(K key) {
            l1.evict(key);
        }

        @Override
        public void invalidateL2(K key) {
            l2.evict(key);
        }

        @Override
        public VersionedValue<V> writeToSource(K key, V value) throws Exception {
            return writer.write(key, value);
        }

        @Override
        public void publishInvalidation(K key, long version) {
            CacheInvalidationEvent event = new CacheInvalidationEvent(
                    UUID.randomUUID().toString(),
                    keyToString.apply(key),
                    version,
                    EventType.INVALIDATE,
                    System.currentTimeMillis());
            publisher.publish(event);
        }

        @Override
        public void scheduleDelayedInvalidation(K key, long delayMillis) {
            // no-op
        }
    }

    private void writeIfNewer(CacheLevel<K, V> level, K key, CacheEntry<V> entry) {
        Optional<CacheEntry<V>> existing = level.getEntry(key);
        if (existing.isPresent() && existing.get().version() > entry.version()) {
            return;
        }
        level.putEntry(key, entry);
    }
}
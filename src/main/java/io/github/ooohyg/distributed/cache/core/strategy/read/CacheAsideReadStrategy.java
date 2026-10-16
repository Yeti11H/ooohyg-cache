package io.github.ooohyg.distributed.cache.core.strategy.read;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.VersionedValue;
import io.github.ooohyg.distributed.cache.api.ReadContext;
import io.github.ooohyg.distributed.cache.core.SingleFlight;

import java.time.Duration;
import java.util.Optional;

/**
 * 默认读策略（Cache Aside）：
 * <pre>
 *   L1 命中 → 返回
 *   L1 miss → L2 命中 → 回填 L1，返回
 *   L2 miss → [可选 SingleFlight] 调 L3 → 回填 L2 + L1，返回
 * </pre>
 */
public class CacheAsideReadStrategy<K, V> implements ReadStrategy<K, V> {

    private final SingleFlight<K, V> singleFlight;

    public CacheAsideReadStrategy() {
        this(null);
    }

    public CacheAsideReadStrategy(SingleFlight<K, V> singleFlight) {
        this.singleFlight = singleFlight;
    }

    @Override
    public Optional<CacheEntry<V>> read(K key, ReadContext<K, V> context) {
        // 1. L1
        Optional<CacheEntry<V>> l1 = context.readL1(key);
        if (l1.isPresent()) {
            context.recordHit("L1");
            return l1;
        }
        context.recordMiss("L1");

        // 2. L2
        Optional<CacheEntry<V>> l2 = context.readL2(key);
        if (l2.isPresent()) {
            context.recordHit("L2");
            context.writeL1(key, l2.get());
            return l2;
        }
        context.recordMiss("L2");

        // 3. L3
        long start = System.currentTimeMillis();
        try {
            VersionedValue<V> loaded;
            if (singleFlight == null) {
                loaded = context.loadFromSource(key);
            } else {
                loaded = singleFlight.execute(key, () -> context.loadFromSource(key));
            }
            context.recordLoadTime(Duration.ofMillis(System.currentTimeMillis() - start));
            if (loaded == null) {
                // L3 明确说不存在——写空值占位符，防止下次穿透
                CacheEntry<V> nullEntry = CacheEntry.empty(0L, context.nullValueTtl());
                context.writeL2(key, nullEntry);
                context.writeL1(key, nullEntry);
                return Optional.empty();
            }
            CacheEntry<V> entry = CacheEntry.from(loaded, context.defaultTtl());
            context.writeL2(key, entry);
            context.writeL1(key, entry);
            return Optional.of(entry);
        } catch (Exception e) {
            context.recordLoadTime(Duration.ofMillis(System.currentTimeMillis() - start));
            return context.handleLoadFailure(key, e);
        }
    }

    public SingleFlight<K, V> singleFlight() {
        return singleFlight;
    }
}
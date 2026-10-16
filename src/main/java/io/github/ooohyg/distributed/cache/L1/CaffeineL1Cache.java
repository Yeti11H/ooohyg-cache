package io.github.ooohyg.distributed.cache.L1;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.api.CacheLevel;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Caffeine 的 L1 本地缓存。
 *
 * <p>两个 TTL 各管各的：
 * <ul>
 *   <li>{@code fallbackTtl}（Caffeine 的 expireAfterWrite）：兜底，防止无限累积；</li>
 *   <li>{@code l1Ttl}：回填 L1 时的实际 TTL，比业务 TTL 短，只留热点。</li>
 * </ul>
 *
 * <p>回填规则：取 min(L1 TTL, 原 entry 剩余时间)。
 * 如果从 L2 回填时业务只剩 2 分钟，L1 TTL 是 5 分钟 → 取 2 分钟。
 * 如果从 L3 回填时业务 TTL 是 30 分钟，L1 TTL 是 5 分钟 → 取 5 分钟。
 */
public class CaffeineL1Cache<K, V> implements CacheLevel<K, V> {

    private static final Duration DEFAULT_FALLBACK_TTL = Duration.ofHours(1);
    private static final Duration DEFAULT_L1_TTL = Duration.ofMinutes(5);

    private final com.github.benmanes.caffeine.cache.Cache<K, CacheEntry<V>> cache;
    private final Duration l1Ttl;
    private final String name;

    public CaffeineL1Cache() {
        this("CaffeineL1Cache", 10_000, DEFAULT_FALLBACK_TTL, DEFAULT_L1_TTL);
    }

    public CaffeineL1Cache(String name, long maximumSize,
                           Duration fallbackTtl, Duration l1Ttl) {
        this.name = name;
        this.l1Ttl = l1Ttl;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(fallbackTtl.toMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public Optional<CacheEntry<V>> getEntry(K key) {
        CacheEntry<V> entry = cache.getIfPresent(key);
        if (entry == null) {
            return Optional.empty();
        }
        if (entry.isPhysicalExpired(System.currentTimeMillis())) {
            cache.invalidate(key);
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    @Override
    public void putEntry(K key, CacheEntry<V> entry) {
        long now = System.currentTimeMillis();
        long l1ExpireAt = now + l1Ttl.toMillis();
        long originalExpireAt = entry.physicalExpireAtMillis();
        long finalExpireAt = originalExpireAt > 0
                ? Math.min(originalExpireAt, l1ExpireAt)
                : l1ExpireAt;
        CacheEntry<V> l1Entry = new CacheEntry<>(
                entry.value(),
                entry.version(),
                entry.nullValue(),
                entry.logicalExpireAtMillis(),
                finalExpireAt
        );
        cache.put(key, l1Entry);
    }

    @Override
    public void evict(K key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public String name() {
        return name;
    }

    public long estimatedSize() {
        return cache.estimatedSize();
    }
}
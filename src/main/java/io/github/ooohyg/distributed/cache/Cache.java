package io.github.ooohyg.distributed.cache;

import java.time.Duration;
import java.util.Optional;

/**
 * 对外统一读写接口。
 */
public interface Cache<K, V> extends AutoCloseable {

    Optional<V> get(K key);

    /**
     * 写入，使用 MultiLevelCache 构造时配置的默认 TTL。
     */
    void put(K key, V value);

    /**
     * 写入，使用业务指定的 TTL，覆盖默认值。
     *
     * <p>注意：Cache Aside 模式下，put 只负责"写数据源 + 删缓存"——
     * TTL 参数会被传递给 WriteStrategy，供"写时更新缓存"的策略使用
     * （如未来的 WriteThroughWriteStrategy）。标准 CacheAsideWriteStrategy
     * 会忽略这个 TTL，因为缓存是在下次读时回填的。
     */
    void put(K key, V value, Duration ttl);

    void invalidate(K key);

    void invalidateAll();

    @Override
    void close();
}
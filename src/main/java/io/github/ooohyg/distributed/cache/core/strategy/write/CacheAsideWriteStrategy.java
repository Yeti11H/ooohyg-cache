package io.github.ooohyg.distributed.cache.core.strategy.write;

import io.github.ooohyg.distributed.cache.VersionedValue;

/**
 * 默认写策略（Cache Aside）：
 * <pre>
 *   put:
 *     1. writeToSource(key, value) → 拿到新 version
 *     2. invalidateL2(key)
 *     3. invalidateL1(key)
 *     4. publishInvalidation(key, version)
 *
 *   invalidate:
 *     1. invalidateL2(key)
 *     2. invalidateL1(key)
 *     3. publishInvalidation(key, 当前版本或 -1)
 * </pre>
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
public class CacheAsideWriteStrategy<K, V> implements WriteStrategy<K, V> {

    @Override
    public void put(K key, V value, WriteContext<K, V> context) {
        try {
            VersionedValue<V> written = context.writeToSource(key, value);
            context.invalidateL2(key);
            context.invalidateL1(key);
            context.publishInvalidation(key, written == null ? -1L : written.version());
        } catch (Exception e) {
            throw new IllegalStateException("write to source failed for key=" + key, e);
        }
    }

    @Override
    public void invalidate(K key, WriteContext<K, V> context) {
        context.invalidateL2(key);
        context.invalidateL1(key);
        context.publishInvalidation(key, -1L);
    }
}

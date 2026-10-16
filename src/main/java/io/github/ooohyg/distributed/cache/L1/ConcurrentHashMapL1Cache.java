package io.github.ooohyg.distributed.cache.L1;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.api.CacheLevel;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于 ConcurrentHashMap 的 L1 本地缓存。
 *
 * <p>用途：
 * <ul>
 *   <li>教学：代码简单，一眼看懂，便于理解 CacheLevel 的语义；</li>
 *   <li>测试：不引入 Caffeine 依赖，单元测试可以轻量运行；</li>
 *   <li>对照：和 CaffeineL1Cache 做性能对比基准。</li>
 * </ul>
 *
 * <p>与 CaffeineL1Cache 的差异：
 * <ul>
 *   <li>没有容量上限——key 无限多时内存会爆；</li>
 *   <li>没有主动淘汰——只在读时惰性清理过期项；</li>
 *   <li>淘汰策略就是"不淘汰"——生产环境不要用它。</li>
 * </ul>
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
public class ConcurrentHashMapL1Cache<K, V> implements CacheLevel<K, V> {

    private final ConcurrentMap<K, CacheEntry<V>> store = new ConcurrentHashMap<>();
    private final String name;

    public ConcurrentHashMapL1Cache() {
        this("ConcurrentHashMapL1Cache");
    }

    public ConcurrentHashMapL1Cache(String name) {
        this.name = name;
    }

    @Override
    public Optional<CacheEntry<V>> getEntry(K key) {
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        // 惰性过期检查：读时判断，过期就顺手清掉
        if (entry.isPhysicalExpired(System.currentTimeMillis())) {
            store.remove(key, entry);
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    @Override
    public void putEntry(K key, CacheEntry<V> entry) {
        store.put(key, entry);
    }

    @Override
    public void evict(K key) {
        store.remove(key);
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * 当前条目数（用于测试断言和调试）。
     */
    public int size() {
        return store.size();
    }
}
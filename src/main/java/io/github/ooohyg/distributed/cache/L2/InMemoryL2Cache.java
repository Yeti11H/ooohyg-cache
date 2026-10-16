package io.github.ooohyg.distributed.cache.L2;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.api.CacheLevel;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 内存 L2 缓存（ConcurrentHashMap）。测试替身：模拟远程缓存。
 *
 * <p>为什么第一版不用 Redis：
 * <ul>
 *   <li>Redis 需要 Docker / 服务端，跑单元测试要额外的环境依赖；</li>
 *   <li>L2 的"网络开销"在多级缓存里的作用是"比 L1 慢、但比 L3 快"——
 *       内存模拟足以验证这个层级语义；</li>
 *   <li>接 Redis 是第三阶段的事，改一行就能换（面向接口编程）。</li>
 * </ul>
 *
 * <p>故障注入：{@link #setAvailable(boolean)} 可以模拟 Redis 挂掉，
 * 用于测试降级路径（第七阶段会用到）。
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
public class InMemoryL2Cache<K, V> implements CacheLevel<K, V> {

    private final ConcurrentMap<K, CacheEntry<V>> store = new ConcurrentHashMap<>();
    private volatile boolean available = true;
    private final String name;

    public InMemoryL2Cache() {
        this("InMemoryL2Cache");
    }

    public InMemoryL2Cache(String name) {
        this.name = name;
    }

    @Override
    public Optional<CacheEntry<V>> getEntry(K key) {
        if (!available) {
            // 模拟"Redis 连接不上"：不抛异常，直接返回空，让编排器走降级路径
            return Optional.empty();
        }
        CacheEntry<V> entry = store.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        if (entry.isPhysicalExpired(System.currentTimeMillis())) {
            store.remove(key, entry);
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    @Override
    public void putEntry(K key, CacheEntry<V> entry) {
        if (available) {
            store.put(key, entry);
        }
        // 不可用时静默忽略写入——模拟"Redis 挂了，回填失败"
    }

    @Override
    public void evict(K key) {
        store.remove(key);
        // evict 即使在不可用时也执行——本地 map 不依赖 Redis 网络
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
     * 故障注入：设为 false 后，get 返回空、put 被忽略。
     */
    public void setAvailable(boolean available) {
        this.available = available;
    }

    public boolean isAvailable() {
        return available;
    }

    /**
     * 当前条目数（用于测试断言）。
     */
    public int size() {
        return store.size();
    }
}
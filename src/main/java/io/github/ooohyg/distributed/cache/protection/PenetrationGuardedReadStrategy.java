package io.github.ooohyg.distributed.cache.protection;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.api.ReadContext;
import io.github.ooohyg.distributed.cache.core.strategy.read.ReadStrategy;

import java.util.Optional;

/**
 * 防穿透读策略装饰器。
 *
 * <p>在委托给内层策略之前，先问布隆过滤器 key 是否可能存在：
 * <ul>
 *   <li>false → 直接返回 empty，不查任何缓存，也不查 L3；</li>
 *   <li>true  → 委托给内层策略（如 CacheAsideReadStrategy）。</li>
 * </ul>
 *
 * <p>用途：把防穿透作为可选能力，用户想加就包一层，不想加就不包。
 * MultiLevelCache 不需要改一行——它只认 ReadStrategy 接口。
 */
public class PenetrationGuardedReadStrategy<K, V> implements ReadStrategy<K, V> {

    private final ReadStrategy<K, V> delegate;
    private final CachePenetrationGuard<K> guard;

    public PenetrationGuardedReadStrategy(ReadStrategy<K, V> delegate,
                                          CachePenetrationGuard<K> guard) {
        this.delegate = delegate;
        this.guard = guard;
    }

    @Override
    public Optional<CacheEntry<V>> read(K key, ReadContext<K, V> context) {
        if (!guard.mightExist(key)) {
            // 布隆过滤器说不存在——直接返回，不查任何缓存
            return Optional.empty();
        }
        return delegate.read(key, context);
    }
}
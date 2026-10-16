package io.github.ooohyg.distributed.cache.core.strategy.read;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.api.ReadContext;

import java.util.Optional;

/**
 * 读策略。决定“先查 L1 还是 L2、命中 L2 是否回填 L1、Loader 失败怎么办”。
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
public interface ReadStrategy<K, V> {

    /**
     * 按策略执行读路径。
     *
     * @return 命中的缓存条目（含版本、空值标记、过期时间等元数据）；返回 empty 表示策略决定不提供值
     */

    Optional<CacheEntry<V>> read(K key, ReadContext<K, V> context);
}

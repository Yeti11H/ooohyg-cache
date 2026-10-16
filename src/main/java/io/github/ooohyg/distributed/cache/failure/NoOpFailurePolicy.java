package io.github.ooohyg.distributed.cache.failure;

import java.util.Optional;

/**
 * 默认降级策略：不做任何降级，全部返回 empty。
 *
 * <p>语义：
 * <ul>
 *   <li>L3 失败 → 业务看到"没有值"（Optional.empty）</li>
 *   <li>某层失败 → 忽略这层，继续往下</li>
 * </ul>
 *
 * <p>使用场景：不允许返回旧值的业务（如支付、订单）。
 */
public class NoOpFailurePolicy<K, V> implements FailurePolicy<K, V> {

    @Override
    public Optional<V> onLoadFailure(K key, Throwable error) {
        return Optional.empty();
    }

    @Override
    public Optional<V> onLevelFailure(K key, String levelName, Throwable error) {
        return Optional.empty();
    }
}
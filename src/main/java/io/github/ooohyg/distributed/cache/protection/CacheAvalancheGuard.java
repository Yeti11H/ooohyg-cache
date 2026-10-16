package io.github.ooohyg.distributed.cache.protection;

import java.time.Duration;

/**
 * 防缓存雪崩。
 *
 * <p>缓存雪崩：大量 key 同时过期导致回源风暴。
 * 本接口只负责"给 TTL 加抖动"——不处理 Redis 整体挂掉的场景
 * （那属于限流/熔断，是第七阶段的事）。
 *
 * <p>设计动机：不同的实现可以有不同的抖动策略。
 * <ul>
 *   <li>固定百分比抖动（±10%）；</li>
 *   <li>热点 key 加成（热点 TTL 更长）；</li>
 *   <li>分段抖动（不同前缀的 key 抖动范围不同）。</li>
 * </ul>
 *
 * @param <K> key 类型
 */
public interface CacheAvalancheGuard<K> {

    /**
     * 计算实际 TTL。
     *
     * @param key     缓存 key（抖动策略可以按 key 区分）
     * @param baseTtl 业务传入的基础 TTL
     * @return 带抖动后的实际 TTL
     */
    Duration computeTtl(K key, Duration baseTtl);
}
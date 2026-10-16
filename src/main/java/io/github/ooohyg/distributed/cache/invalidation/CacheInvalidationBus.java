package io.github.ooohyg.distributed.cache.invalidation;

import io.github.ooohyg.distributed.cache.invalidation.CacheInvalidationPublisher;
import io.github.ooohyg.distributed.cache.invalidation.CacheInvalidationSubscriber;

/**
 * 内存总线标记接口：既支持发布，也支持本地订阅。
 *
 * <p>为什么继承 {@link CacheInvalidationPublisher}：
 * 总线本来就"能发布"，继承它省去重复声明。
 * 额外的 {@link #subscribe} 方法让总线"能注册订阅者"。
 *
 * <p>使用场景：
 * 第一版内存实现中，同一个对象既是发布者又是订阅者容器——
 * MultiLevelCache 构造时如果发现 publisher 是 CacheInvalidationBus，
 * 会自动把自己注册为订阅者。
 *
 * <p>生产环境接入 Redis Pub/Sub 时，publisher 只实现
 * {@link CacheInvalidationPublisher}——订阅由 Redis 客户端自己处理，
 * 不需要实现这个接口。
 */
public interface CacheInvalidationBus extends CacheInvalidationPublisher {

    /**
     * 注册订阅者。
     */
    void subscribe(CacheInvalidationSubscriber subscriber);
}
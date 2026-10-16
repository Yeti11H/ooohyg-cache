package io.github.ooohyg.distributed.cache.invalidation;

import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;

/**
 * 失效事件发布器。负责把失效事件广播给订阅方（其他实例 / 本地总线）。
 */
public interface CacheInvalidationPublisher {

    /**
     * 发布失效事件。
     */
    void publish(CacheInvalidationEvent event);
}

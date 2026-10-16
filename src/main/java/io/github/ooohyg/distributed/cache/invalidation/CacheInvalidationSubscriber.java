package io.github.ooohyg.distributed.cache.invalidation;

import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;

/**
 * 失效事件订阅器。收到事件后仅使本地缓存失效，下次读取从数据源重新加载。
 */
public interface CacheInvalidationSubscriber {

    /**
     * 处理失效事件。
     */
    void onEvent(CacheInvalidationEvent event);
}

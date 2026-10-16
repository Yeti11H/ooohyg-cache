package io.github.ooohyg.distributed.cache.invalidation;

import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 内存失效事件总线。测试替身：同一 JVM 内同步广播，用于单机场景。
 *
 * <p>与 RedisPubSubInvalidationBus 的差异：
 * <ul>
 *   <li>本类——单 JVM，同步，无序列化；</li>
 *   <li>Redis 版——跨 JVM，异步，需要序列化。</li>
 * </ul>
 *
 * <p>CopyOnWriteArrayList 的理由：注册订阅者（写）很少，
 * 发布事件（读）很多——读操作无锁，适合这种场景。
 */
public class InMemoryInvalidationBus implements CacheInvalidationBus {

    private final List<CacheInvalidationSubscriber> subscribers = new CopyOnWriteArrayList<>();

    @Override
    public void subscribe(CacheInvalidationSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    public void unsubscribe(CacheInvalidationSubscriber subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    public void publish(CacheInvalidationEvent event) {
        for (CacheInvalidationSubscriber subscriber : subscribers) {
            try {
                subscriber.onEvent(event);
            } catch (Exception e) {
                // 单个订阅者失败不影响其他订阅者
                // 第一版只吞掉；第八阶段接入日志后改成结构化日志
            }
        }
    }

    public int subscriberCount() {
        return subscribers.size();
    }
}
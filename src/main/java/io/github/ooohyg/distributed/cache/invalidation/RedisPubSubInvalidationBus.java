package io.github.ooohyg.distributed.cache.invalidation;

import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import io.github.ooohyg.distributed.cache.serializer.CacheSerializer;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 基于 Redis Pub/Sub 的失效广播总线。
 *
 * <p>职责：
 * <ul>
 *   <li>作为 {@link CacheInvalidationPublisher}：把事件序列化后 publish 到 Redis 频道；</li>
 *   <li>作为 {@link CacheInvalidationBus}：注册本地订阅者，收到 Redis 消息后转发给它们。</li>
 * </ul>
 *
 * <p>为什么需要两个 Lettuce 连接：
 * 订阅是阻塞模型——连接订阅后只能收消息，不能再执行普通命令。
 * 所以发布用普通连接，订阅用专门的 Pub/Sub 连接。
 *
 * <p>线程安全：
 * subscribers 用 CopyOnWriteArrayList——读多写少场景（订阅注册一次，收消息频繁读）。
 * 收到消息时即使某个 subscriber 抛异常，也不影响其他 subscriber。
 */
public class RedisPubSubInvalidationBus implements CacheInvalidationBus {

    private final RedisCommands<String, byte[]> publishCommands;
    private final StatefulRedisPubSubConnection<String, byte[]> subscribeConnection;
    private final CacheSerializer<CacheInvalidationEvent> serializer;
    private final String channel;
    private final List<CacheInvalidationSubscriber> subscribers = new CopyOnWriteArrayList<>();

    public RedisPubSubInvalidationBus(
            RedisCommands<String, byte[]> publishCommands,
            StatefulRedisPubSubConnection<String, byte[]> subscribeConnection,
            CacheSerializer<CacheInvalidationEvent> serializer,
            String channel) {
        this.publishCommands = publishCommands;
        this.subscribeConnection = subscribeConnection;
        this.serializer = serializer;
        this.channel = channel;
        startSubscribe();
    }

    /**
     * 启动后台订阅。Lettuce 在内部线程回调 listener，
     * 我们在这里把收到的 byte[] 反序列化，再转发给本地订阅者。
     */
    private void startSubscribe() {
        subscribeConnection.addListener(new RedisPubSubAdapter<String, byte[]>() {
            @Override
            public void message(String channel, byte[] message) {
                if (!RedisPubSubInvalidationBus.this.channel.equals(channel)) {
                    return;   // 不是我们关心的频道（防止误订阅）
                }
                CacheInvalidationEvent event = serializer.deserialize(message);
                for (CacheInvalidationSubscriber subscriber : subscribers) {
                    try {
                        subscriber.onEvent(event);
                    } catch (Exception e) {
                        // 单个订阅者失败不影响其他订阅者
                        // 第一版只吞掉异常；第七阶段接入 FailurePolicy 后再处理
                    }
                }
            }
        });
        subscribeConnection.sync().subscribe(channel);
    }

    @Override
    public void publish(CacheInvalidationEvent event) {
        byte[] bytes = serializer.serialize(event);
        publishCommands.publish(channel, bytes);
    }

    @Override
    public void subscribe(CacheInvalidationSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    /**
     * 关闭时取消订阅、断开连接。
     * 注意：这里不关 publishCommands 的连接——那是共享的普通连接，不应该由本类关闭。
     */
    public void close() {
        subscribers.clear();
        try {
            subscribeConnection.sync().unsubscribe(channel);
        } finally {
            subscribeConnection.close();
        }
    }
}
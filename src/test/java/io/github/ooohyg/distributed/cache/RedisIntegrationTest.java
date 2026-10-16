package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.L2.RedisL2Cache;
import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import io.github.ooohyg.distributed.cache.invalidation.RedisPubSubInvalidationBus;
import io.github.ooohyg.distributed.cache.serializer.JacksonCacheSerializer;
import io.github.ooohyg.distributed.cache.serializer.JacksonEventSerializer;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redis 集成模块测试（Lettuce fake 版，无需真实 Redis）：
 * RedisL2Cache 的 get/set/pexpire/evict/TTL 缓冲逻辑，
 * RedisPubSubInvalidationBus 的订阅发布转发链路。
 *
 * <p>注意：序列化器使用 {@link SerializerModuleTest#fixedMapper()} 的 MixIn 修复配置，
 * 因为项目默认 ObjectMapper 存在 FAIL_ON_EMPTY_BEANS 缺陷（见 SerializerModuleTest）。
 */
class RedisIntegrationTest {

    // ---------------- Lettuce fake ----------------

    static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0f;
        }
        if (type == double.class) {
            return 0d;
        }
        return null;
    }

    static class FakeRedisState {
        final Map<String, byte[]> store = new ConcurrentHashMap<>();
        final Map<String, Long> pexpires = new ConcurrentHashMap<>();
        final AtomicInteger publishes = new AtomicInteger();
    }

    @SuppressWarnings("unchecked")
    static RedisCommands<String, byte[]> fakeCommands(FakeRedisState state) {
        return (RedisCommands<String, byte[]>) Proxy.newProxyInstance(
                RedisCommands.class.getClassLoader(),
                new Class<?>[]{RedisCommands.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "get":
                            return state.store.get((String) args[0]);
                        case "set":
                            state.store.put((String) args[0], (byte[]) args[1]);
                            return "OK";
                        case "pexpire":
                            state.pexpires.put((String) args[0], (Long) args[1]);
                            return true;
                        case "del": {
                            Object k0 = args[0];
                            if (k0 instanceof String[]) {
                                for (Object k : (String[]) k0) {
                                    state.store.remove((String) k);
                                }
                            } else {
                                state.store.remove((String) k0);
                            }
                            return 1L;
                        }
                        case "publish":
                            state.publishes.incrementAndGet();
                            return 1L;
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    static class FakePubSubState extends FakeRedisState {
        volatile RedisPubSubListener<String, byte[]> listener;
        final CopyOnWriteArrayList<String> subscribedChannels = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> unsubscribedChannels = new CopyOnWriteArrayList<>();
    }

    @SuppressWarnings("unchecked")
    static StatefulRedisPubSubConnection<String, byte[]> fakePubSubConnection(FakePubSubState state) {
        return (StatefulRedisPubSubConnection<String, byte[]>)
                Proxy.newProxyInstance(
                        StatefulRedisPubSubConnection.class.getClassLoader(),
                        new Class<?>[]{StatefulRedisPubSubConnection.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "addListener":
                                    state.listener = (RedisPubSubListener<String, byte[]>) args[0];
                                    return null;
                                case "sync":
                                    return Proxy.newProxyInstance(
                                            RedisCommands.class.getClassLoader(),
                                            new Class<?>[]{io.lettuce.core.pubsub.api.sync.RedisPubSubCommands.class},
                                            (syncProxy, syncMethod, syncArgs) -> {
                                                switch (syncMethod.getName()) {
                                                    case "subscribe": {
                                                        String[] channels = (String[]) syncArgs[0];
                                                        for (String ch : channels) {
                                                            state.subscribedChannels.add(ch);
                                                        }
                                                        return null;
                                                    }
                                                    case "unsubscribe": {
                                                        String[] channels = (String[]) syncArgs[0];
                                                        for (String ch : channels) {
                                                            state.unsubscribedChannels.add(ch);
                                                        }
                                                        return null;
                                                    }
                                                    default:
                                                        return defaultValue(syncMethod.getReturnType());
                                                }
                                            });
                                case "close":
                                    return null;
                                default:
                                    return defaultValue(method.getReturnType());
                            }
                        });
    }

    /** 模拟 Redis 服务端把 publish 的消息广播回订阅连接。 */
    static void simulateServerDelivery(FakePubSubState state, String channel, byte[] payload) {
        assertNotNull(state.listener, "总线构造时应已 addListener");
        state.listener.message(channel, payload);
    }

    // ---------------- RedisL2Cache ----------------

    @Test
    void redisL2_putAndGet_roundTrip() {
        FakeRedisState state = new FakeRedisState();
        RedisCommands<String, byte[]> commands = fakeCommands(state);

        JacksonCacheSerializer<String> serializer =
                new JacksonCacheSerializer<String>(SerializerModuleTest.fixedMapper(), String.class);
        RedisL2Cache<String, String> l2 = new RedisL2Cache<String, String>(
                commands, serializer, "cache:", Function.identity(), "redis-l2");

        CacheEntry<String> entry = CacheEntry.from(
                new VersionedValue<>("value-1", 7L), Duration.ofMinutes(30));

        l2.putEntry("order:1", entry);

        assertTrue(state.store.containsKey("cache:order:1"), "key 应带前缀写入");
        Optional<CacheEntry<String>> got = l2.getEntry("order:1");
        assertTrue(got.isPresent());
        assertEquals(entry, got.get(), "往返后 CacheEntry 应一致");
        assertTrue(state.pexpires.containsKey("cache:order:1"), "应设置 Redis TTL");
        long expectedTtl = Duration.ofMinutes(30).toMillis() + Duration.ofMinutes(1).toMillis();
        long actualTtl = state.pexpires.get("cache:order:1");
        assertTrue(actualTtl <= expectedTtl && actualTtl >= expectedTtl - 5000,
                "Redis TTL = 业务 TTL + 1 分钟缓冲（构造到写入间存在毫秒级误差），实际=" + actualTtl);
    }

    @Test
    void redisL2_miss_returnsEmpty() {
        FakeRedisState state = new FakeRedisState();
        RedisL2Cache<String, String> l2 = new RedisL2Cache<String, String>(
                fakeCommands(state),
                new JacksonCacheSerializer<String>(SerializerModuleTest.fixedMapper(), String.class),
                "cache:", Function.identity(), "redis-l2");
        assertEquals(Optional.empty(), l2.getEntry("missing"));
    }

    @Test
    void redisL2_evict_deletesKey() {
        FakeRedisState state = new FakeRedisState();
        RedisL2Cache<String, String> l2 = new RedisL2Cache<String, String>(
                fakeCommands(state),
                new JacksonCacheSerializer<String>(SerializerModuleTest.fixedMapper(), String.class),
                "cache:", Function.identity(), "redis-l2");

        l2.putEntry("k", CacheEntry.from(new VersionedValue<>("v", 1L), Duration.ofMinutes(30)));
        assertTrue(state.store.containsKey("cache:k"));

        l2.evict("k");
        assertFalse(state.store.containsKey("cache:k"));
        assertEquals(Optional.empty(), l2.getEntry("k"));
    }

    @Test
    void redisL2_noBusinessTtl_usesFallbackTtl() {
        FakeRedisState state = new FakeRedisState();
        RedisL2Cache<String, String> l2 = new RedisL2Cache<String, String>(
                fakeCommands(state),
                new JacksonCacheSerializer<String>(SerializerModuleTest.fixedMapper(), String.class),
                "cache:", Function.identity(), "redis-l2");

        // 无业务 TTL（physicalExpireAtMillis = 0，永不过期）
        l2.putEntry("k", CacheEntry.from(new VersionedValue<>("v", 1L)));

        long fallback = Duration.ofHours(24).toMillis() + Duration.ofMinutes(1).toMillis();
        assertEquals(fallback, state.pexpires.get("cache:k"),
                "无业务 TTL 时使用 24h fallback + 1min 缓冲");
    }

    @Test
    void redisL2_expiredEntry_lazilyDeleted() {
        FakeRedisState state = new FakeRedisState();
        RedisL2Cache<String, String> l2 = new RedisL2Cache<String, String>(
                fakeCommands(state),
                new JacksonCacheSerializer<String>(SerializerModuleTest.fixedMapper(), String.class),
                "cache:", Function.identity(), "redis-l2");

        // 已物理过期的 entry（physicalExpireAtMillis 已到）
        l2.putEntry("expired", new CacheEntry<>("old", 1L, false, 0L,
                System.currentTimeMillis() - 1000));

        assertTrue(state.store.containsKey("cache:expired"), "写入时仍在（由 Redis TTL 兜底）");
        assertEquals(Optional.empty(), l2.getEntry("expired"), "读取时惰性删除并返回空");
        assertFalse(state.store.containsKey("cache:expired"), "惰性删除后不应残留");
    }

    @Test
    void redisL2_clear_throwsUnsupported() {
        FakeRedisState state = new FakeRedisState();
        RedisL2Cache<String, String> l2 = new RedisL2Cache<String, String>(
                fakeCommands(state),
                new JacksonCacheSerializer<String>(SerializerModuleTest.fixedMapper(), String.class),
                "cache:", Function.identity(), "redis-l2");
        assertThrows(UnsupportedOperationException.class, l2::clear,
                "Redis L2 明确不支持 clear，只能逐 key evict");
    }

    // ---------------- RedisPubSubInvalidationBus ----------------

    @Test
    void bus_subscribesOnStart_andPublishForwardsToSubscribers() {
        FakePubSubState state = new FakePubSubState();
        RedisCommands<String, byte[]> publishCommands = fakeCommands(state);
        StatefulRedisPubSubConnection<String, byte[]> subConn = fakePubSubConnection(state);

        JacksonEventSerializer serializer =
                new JacksonEventSerializer(SerializerModuleTest.fixedMapper());
        RedisPubSubInvalidationBus bus = new RedisPubSubInvalidationBus(
                publishCommands, subConn, serializer, "cache-invalidation");

        assertEquals(1, state.subscribedChannels.size(), "启动时应订阅目标频道");
        assertEquals("cache-invalidation", state.subscribedChannels.get(0));

        // 注册本地订阅者
        CopyOnWriteArrayList<CacheInvalidationEvent> received = new CopyOnWriteArrayList<>();
        bus.subscribe(received::add);

        CacheInvalidationEvent event = new CacheInvalidationEvent(
                "evt-x", "order:9", 3L, EventType.INVALIDATE, 1_700_000_000_000L);

        bus.publish(event);

        assertEquals(1, state.publishes.get(), "publish 应调用 Redis publish");
        // 模拟 Redis 广播回来
        simulateServerDelivery(state, "cache-invalidation", serializer.serialize(event));

        assertEquals(1, received.size(), "订阅者应收到反序列化后的事件");
        assertEquals(event, received.get(0));

        bus.close();
        assertTrue(state.unsubscribedChannels.contains("cache-invalidation"),
                "close 应取消订阅");
    }

    @Test
    void bus_ignoresMessagesFromOtherChannels() {
        FakePubSubState state = new FakePubSubState();
        RedisPubSubInvalidationBus bus = new RedisPubSubInvalidationBus(
                fakeCommands(state), fakePubSubConnection(state),
                new JacksonEventSerializer(SerializerModuleTest.fixedMapper()),
                "my-channel");

        CopyOnWriteArrayList<CacheInvalidationEvent> received = new CopyOnWriteArrayList<>();
        bus.subscribe(received::add);

        CacheInvalidationEvent other = new CacheInvalidationEvent(
                "evt-other", "k", 1L, EventType.INVALIDATE, 1L);
        simulateServerDelivery(state, "other-channel",
                new JacksonEventSerializer(SerializerModuleTest.fixedMapper()).serialize(other));

        assertEquals(0, received.size(), "其它频道消息不应转发");
        bus.close();
    }

    @Test
    void bus_subscriberException_doesNotBlockOthers() {
        FakePubSubState state = new FakePubSubState();
        JacksonEventSerializer serializer = new JacksonEventSerializer(SerializerModuleTest.fixedMapper());
        RedisPubSubInvalidationBus bus = new RedisPubSubInvalidationBus(
                fakeCommands(state), fakePubSubConnection(state), serializer, "ch");

        CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();
        bus.subscribe(evt -> {
            order.add("first");
            throw new RuntimeException("boom");
        });
        bus.subscribe(evt -> order.add("second"));

        CacheInvalidationEvent event = new CacheInvalidationEvent(
                "evt-z", "k", 1L, EventType.INVALIDATE, 1L);
        simulateServerDelivery(state, "ch", serializer.serialize(event));

        assertEquals(2, order.size(), "异常订阅者不应阻塞后续订阅者");
        assertEquals("first", order.get(0));
        assertEquals("second", order.get(1));
        bus.close();
    }
}

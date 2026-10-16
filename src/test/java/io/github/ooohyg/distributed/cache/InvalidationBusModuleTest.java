package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import io.github.ooohyg.distributed.cache.invalidation.CacheInvalidationSubscriber;
import io.github.ooohyg.distributed.cache.invalidation.InMemoryInvalidationBus;
import io.github.ooohyg.distributed.cache.serializer.JacksonEventSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 失效广播模块测试：InMemoryInvalidationBus 的订阅/发布/异常隔离。
 */
class InvalidationBusModuleTest {

    static class CountingSubscriber implements CacheInvalidationSubscriber {
        final List<CacheInvalidationEvent> received = new CopyOnWriteArrayList<>();
        final boolean throwOnEvent;
        CountingSubscriber(boolean throwOnEvent) { this.throwOnEvent = throwOnEvent; }

        @Override public void onEvent(CacheInvalidationEvent event) {
            received.add(event);
            if (throwOnEvent) {
                throw new RuntimeException("subscriber bug");
            }
        }
    }

    private CacheInvalidationEvent event(String id, long version) {
        return new CacheInvalidationEvent(id, "k", version, EventType.INVALIDATE,
                System.currentTimeMillis());
    }

    @Test
    void inMemoryBus_deliversToAllSubscribers() {
        InMemoryInvalidationBus bus = new InMemoryInvalidationBus();
        CountingSubscriber a = new CountingSubscriber(false);
        CountingSubscriber b = new CountingSubscriber(false);
        bus.subscribe(a);
        bus.subscribe(b);

        bus.publish(event("e1", 1L));

        assertEquals(1, a.received.size());
        assertEquals(1, b.received.size());
        assertEquals("e1", a.received.get(0).eventId());
    }

    @Test
    void inMemoryBus_subscriberException_doesNotBlockOthers() {
        InMemoryInvalidationBus bus = new InMemoryInvalidationBus();
        CountingSubscriber bad = new CountingSubscriber(true);
        CountingSubscriber good = new CountingSubscriber(false);
        bus.subscribe(bad);
        bus.subscribe(good);

        // 不应抛异常
        bus.publish(event("e1", 1L));

        assertEquals(1, good.received.size(), "异常订阅者不应影响其他订阅者");
        assertEquals(1, bad.received.size(), "异常订阅者自身也应收到事件（异常发生在处理阶段）");
    }

    @Test
    void inMemoryBus_unsubscribe_stopsDelivery() {
        InMemoryInvalidationBus bus = new InMemoryInvalidationBus();
        CountingSubscriber a = new CountingSubscriber(false);
        bus.subscribe(a);
        bus.publish(event("e1", 1L));
        bus.unsubscribe(a);
        bus.publish(event("e2", 1L));
        assertEquals(1, a.received.size(), "取消订阅后不再收到事件");
        assertEquals(0, bus.subscriberCount());
    }

    @Test
    void inMemoryBus_publishWithNoSubscribers_isSafe() {
        InMemoryInvalidationBus bus = new InMemoryInvalidationBus();
        assertDoesNotThrow(() -> bus.publish(event("e1", 1L)));
    }
}

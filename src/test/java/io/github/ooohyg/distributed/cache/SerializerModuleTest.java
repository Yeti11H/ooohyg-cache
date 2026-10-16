package io.github.ooohyg.distributed.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import io.github.ooohyg.distributed.cache.serializer.JacksonCacheSerializer;
import io.github.ooohyg.distributed.cache.serializer.JacksonEventSerializer;
import io.github.ooohyg.distributed.cache.serializer.SerializationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 序列化模块测试。
 *
 * <p><b>生产修复已落地</b>：CacheEntry / CacheInvalidationEvent 已通过
 * {@code @JsonCreator + @JsonProperty} 显式声明序列化契约，因此
 * JacksonCacheSerializer / JacksonEventSerializer 使用<b>默认 ObjectMapper</b>
 * （即生产真实路径）即可完成序列化往返。
 *
 * <p>测试策略：用例验证默认 ObjectMapper（生产配置）下
 * CacheEntry / CacheInvalidationEvent 能完整往返、正确处理空值占位符，
 * 并保留 MixIn 修复方案用例作为对照参考。
 */
class SerializerModuleTest {

    // ---------------- MixIn 修复方案（供报告引用） ----------------

    abstract static class CacheEntryMixin<V> {
        @JsonProperty("value") abstract V value();
        @JsonProperty("version") abstract long version();
        @JsonProperty("nullValue") abstract boolean nullValue();
        @JsonProperty("logicalExpireAtMillis") abstract long logicalExpireAtMillis();
        @JsonProperty("physicalExpireAtMillis") abstract long physicalExpireAtMillis();

        @JsonCreator
        CacheEntryMixin(@JsonProperty("value") V value,
                        @JsonProperty("version") long version,
                        @JsonProperty("nullValue") boolean nullValue,
                        @JsonProperty("logicalExpireAtMillis") long logicalExpireAtMillis,
                        @JsonProperty("physicalExpireAtMillis") long physicalExpireAtMillis) {
        }
    }

    abstract static class EventMixin {
        @JsonProperty("eventId") abstract String eventId();
        @JsonProperty("key") abstract String key();
        @JsonProperty("version") abstract long version();
        @JsonProperty("eventType") abstract EventType eventType();
        @JsonProperty("timestampMillis") abstract long timestampMillis();

        @JsonCreator
        EventMixin(@JsonProperty("eventId") String eventId,
                   @JsonProperty("key") String key,
                   @JsonProperty("version") long version,
                   @JsonProperty("eventType") EventType eventType,
                   @JsonProperty("timestampMillis") long timestampMillis) {
        }
    }

    static ObjectMapper fixedMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.addMixIn(CacheEntry.class, CacheEntryMixin.class);
        mapper.addMixIn(CacheInvalidationEvent.class, EventMixin.class);
        return mapper;
    }

    // ---------------- 生产默认路径（修复后裸 ObjectMapper 即可用） ----------------

    @Test
    void defaultMapper_cacheEntry_roundTrip_preservesAllFields() {
        JacksonCacheSerializer<String> serializer =
                new JacksonCacheSerializer<>(new ObjectMapper(), String.class);

        CacheEntry<String> original = CacheEntry.from(
                new VersionedValue<>("hello", 5L), Duration.ofMinutes(30));

        byte[] bytes = serializer.serialize(original);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        CacheEntry<String> restored = serializer.deserialize(bytes);
        assertEquals(original, restored, "往返后 CacheEntry 应完全一致");
        assertEquals("hello", restored.value());
        assertEquals(5L, restored.version());
        assertFalse(restored.nullValue());
        assertTrue(restored.physicalExpireAtMillis() > System.currentTimeMillis());
    }

    @Test
    void defaultMapper_event_roundTrip_preservesEvent() {
        JacksonEventSerializer serializer = new JacksonEventSerializer(new ObjectMapper());

        CacheInvalidationEvent event = new CacheInvalidationEvent(
                "evt-001", "order:1", 9L, EventType.INVALIDATE, 1_700_000_000_000L);

        CacheInvalidationEvent restored = serializer.deserialize(serializer.serialize(event));
        assertEquals(event, restored, "事件往返后应完全一致");
        assertEquals(EventType.INVALIDATE, restored.eventType());
        assertEquals(9L, restored.version());
    }

    // ---------------- MixIn 修复后的正确行为 ----------------

    @Test
    void fixedMapper_cacheEntry_roundTrip_preservesAllFields() {
        JacksonCacheSerializer<String> serializer =
                new JacksonCacheSerializer<>(fixedMapper(), String.class);

        CacheEntry<String> original = CacheEntry.from(
                new VersionedValue<>("hello", 5L), Duration.ofMinutes(30));

        byte[] bytes = serializer.serialize(original);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        CacheEntry<String> restored = serializer.deserialize(bytes);
        assertEquals(original, restored, "往返后 CacheEntry 应完全一致");
        assertEquals("hello", restored.value());
        assertEquals(5L, restored.version());
        assertFalse(restored.nullValue());
        assertTrue(restored.physicalExpireAtMillis() > System.currentTimeMillis());
    }

    @Test
    void fixedMapper_cacheEntry_roundTrip_nullValueEntry() {
        JacksonCacheSerializer<String> serializer =
                new JacksonCacheSerializer<>(fixedMapper(), String.class);

        CacheEntry<String> original = CacheEntry.empty(0L, Duration.ofMinutes(1));

        CacheEntry<String> restored = serializer.deserialize(serializer.serialize(original));
        assertEquals(original, restored);
        assertTrue(restored.nullValue());
        assertNull(restored.value());
    }

    @Test
    void fixedMapper_event_roundTrip_preservesEvent() {
        JacksonEventSerializer serializer = new JacksonEventSerializer(fixedMapper());

        CacheInvalidationEvent event = new CacheInvalidationEvent(
                "evt-001", "order:1", 9L, EventType.INVALIDATE, 1_700_000_000_000L);

        CacheInvalidationEvent restored = serializer.deserialize(serializer.serialize(event));
        assertEquals(event, restored, "事件往返后应完全一致");
        assertEquals(EventType.INVALIDATE, restored.eventType());
        assertEquals(9L, restored.version());
    }

    @Test
    void fixedMapper_event_roundTrip_putTypeEvent() {
        JacksonEventSerializer serializer = new JacksonEventSerializer(fixedMapper());

        CacheInvalidationEvent event = new CacheInvalidationEvent(
                "evt-002", "user:42", 3L, EventType.PUT, 1_700_000_000_000L);

        CacheInvalidationEvent restored = serializer.deserialize(serializer.serialize(event));
        assertEquals(EventType.PUT, restored.eventType());
        assertEquals("user:42", restored.key());
    }

    @Test
    void fixedMapper_handlesMalformedBytes() {
        JacksonCacheSerializer<String> serializer =
                new JacksonCacheSerializer<>(fixedMapper(), String.class);
        assertThrows(SerializationException.class,
                () -> serializer.deserialize(new byte[] {1, 2, 3}));
    }
}

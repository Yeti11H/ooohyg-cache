package io.github.ooohyg.distributed.cache.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.ooohyg.distributed.cache.EventType;

import java.util.Objects;

/**
 * 失效广播事件。
 *
 * <p>关键设计：
 * <ul>
 *   <li>{@code eventId} 用于幂等：Subscriber 维护 seenEventIds，重复事件直接丢弃。</li>
 *   <li>{@code version} 用于防旧覆盖新：本地已有 version=10，收到 version=9 的事件丢弃。</li>
 *   <li>{@code key} 统一使用 String：跨节点传输必须可序列化，业务 Key 必须能编码成字符串。</li>
 * </ul>
 */
public final class CacheInvalidationEvent {

    private final String eventId;
    private final String key;
    private final long version;
    private final EventType eventType;
    private final long timestampMillis;

    @JsonCreator
    public CacheInvalidationEvent(@JsonProperty("eventId") String eventId,
                                  @JsonProperty("key") String key,
                                  @JsonProperty("version") long version,
                                  @JsonProperty("eventType") EventType eventType,
                                  @JsonProperty("timestampMillis") long timestampMillis) {
        if (eventId == null || eventId.isEmpty()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        if (eventType == null) {
            throw new IllegalArgumentException("eventType must not be null");
        }
        this.eventId = eventId;
        this.key = key;
        this.version = version;
        this.eventType = eventType;
        this.timestampMillis = timestampMillis;
    }

    @JsonProperty("eventId")
    public String eventId() {
        return eventId;
    }

    @JsonProperty("key")
    public String key() {
        return key;
    }

    @JsonProperty("version")
    public long version() {
        return version;
    }

    @JsonProperty("eventType")
    public EventType eventType() {
        return eventType;
    }

    @JsonProperty("timestampMillis")
    public long timestampMillis() {
        return timestampMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CacheInvalidationEvent)) {
            return false;
        }
        CacheInvalidationEvent that = (CacheInvalidationEvent) o;
        return version == that.version
                && timestampMillis == that.timestampMillis
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(key, that.key)
                && eventType == that.eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, key, version, eventType, timestampMillis);
    }

    @Override
    public String toString() {
        return "CacheInvalidationEvent{eventId='" + eventId + "', key='" + key
                + "', version=" + version + ", eventType=" + eventType
                + ", timestampMillis=" + timestampMillis + '}';
    }
}

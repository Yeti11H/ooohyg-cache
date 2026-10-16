package io.github.ooohyg.distributed.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.util.Objects;

/**
 * "缓存里的条目 + 元数据"，L1 / L2 内部存储单位。
 *
 * <p>与 VersionedValue 的职责边界：
 * <ul>
 *   <li>VersionedValue 只表示"值 + 版本"（数据源/事件用）；</li>
 *   <li>CacheEntry 表示"值/空值、版本、逻辑过期、物理过期"（缓存内用）。</li>
 * </ul>
 */
public final class CacheEntry<V> {

    private final V value;
    private final long version;
    private final boolean nullValue;
    private final long logicalExpireAtMillis;
    private final long physicalExpireAtMillis;

    @JsonCreator
    public CacheEntry(@JsonProperty("value") V value,
                      @JsonProperty("version") long version,
                      @JsonProperty("nullValue") boolean nullValue,
                      @JsonProperty("logicalExpireAtMillis") long logicalExpireAtMillis,
                      @JsonProperty("physicalExpireAtMillis") long physicalExpireAtMillis) {
        this.value = value;
        this.version = version;
        this.nullValue = nullValue;
        this.logicalExpireAtMillis = logicalExpireAtMillis;
        this.physicalExpireAtMillis = physicalExpireAtMillis;
    }

    @JsonProperty("value")
    public V value() {
        return value;
    }

    @JsonProperty("version")
    public long version() {
        return version;
    }

    @JsonProperty("nullValue")
    public boolean nullValue() {
        return nullValue;
    }

    @JsonProperty("logicalExpireAtMillis")
    public long logicalExpireAtMillis() {
        return logicalExpireAtMillis;
    }

    @JsonProperty("physicalExpireAtMillis")
    public long physicalExpireAtMillis() {
        return physicalExpireAtMillis;
    }

    /**
     * 是否物理过期。physicalExpireAtMillis &lt;= 0 表示永不过期。
     */
    public boolean isPhysicalExpired(long nowMillis) {
        return physicalExpireAtMillis > 0 && nowMillis >= physicalExpireAtMillis;
    }

    /**
     * 是否逻辑过期（逻辑过期时间 > 0 且已到达）。
     */
    public boolean isLogicalExpired(long nowMillis) {
        return logicalExpireAtMillis > 0 && nowMillis >= logicalExpireAtMillis;
    }

    /**
     * 由数据源结果构造缓存条目，无 TTL（永不过期）。
     */
    public static <V> CacheEntry<V> from(VersionedValue<V> source) {
        return new CacheEntry<>(source.value(), source.version(),
                source.value() == null, 0L, 0L);
    }

    /**
     * 由数据源结果构造缓存条目，附带 TTL。
     * physicalExpireAtMillis = now + ttl.toMillis()。
     */
    public static <V> CacheEntry<V> from(VersionedValue<V> source, Duration ttl) {
        long expireAt = (ttl == null || ttl.isZero() || ttl.isNegative())
                ? 0L
                : System.currentTimeMillis() + ttl.toMillis();
        return new CacheEntry<>(source.value(), source.version(),
                source.value() == null, 0L, expireAt);
    }

    /**
     * 构造空值占位符，附带 TTL。
     *
     * <p>用途：L3 明确返回"不存在"时，写一份空值占位符到缓存，
     * 防止下一次查询又穿到 L3。
     *
     * @param version 空值占位符的版本号，通常用 0
     * @param ttl     空值的存活时间，应该远短于正常值（默认 1 分钟）
     */
    public static <V> CacheEntry<V> empty(long version, Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("null value ttl must be positive");
        }
        long expireAt = System.currentTimeMillis() + ttl.toMillis();
        return new CacheEntry<>(null, version, true, 0L, expireAt);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CacheEntry)) {
            return false;
        }
        CacheEntry<?> that = (CacheEntry<?>) o;
        return version == that.version
                && nullValue == that.nullValue
                && logicalExpireAtMillis == that.logicalExpireAtMillis
                && physicalExpireAtMillis == that.physicalExpireAtMillis
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, version, nullValue,
                logicalExpireAtMillis, physicalExpireAtMillis);
    }

    @Override
    public String toString() {
        return "CacheEntry{value=" + value + ", version=" + version
                + ", nullValue=" + nullValue
                + ", logicalExpireAtMillis=" + logicalExpireAtMillis
                + ", physicalExpireAtMillis=" + physicalExpireAtMillis + '}';
    }
}
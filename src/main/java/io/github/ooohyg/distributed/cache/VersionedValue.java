package io.github.ooohyg.distributed.cache;

import java.util.Objects;

/**
 * “数据源里的值 + 版本”，由 Loader 返回，也是失效事件的版本真源。
 *
 * <p>version 使用 long 且规定单调递增（不使用 Instant，避免时钟回拨破坏比较）。
 *
 * @param <V> value 类型
 */
public final class VersionedValue<V> {

    private final V value;
    private final long version;

    public VersionedValue(V value, long version) {
        if (version < 0) {
            throw new IllegalArgumentException("version must be >= 0, but was " + version);
        }
        this.value = value;
        this.version = version;
    }

    public V value() {
        return value;
    }

    public long version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VersionedValue)) {
            return false;
        }
        VersionedValue<?> that = (VersionedValue<?>) o;
        return version == that.version && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, version);
    }

    @Override
    public String toString() {
        return "VersionedValue{value=" + value + ", version=" + version + '}';
    }
}

package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 值模型模块测试：VersionedValue / CacheEntry / CacheInvalidationEvent 的构造与语义。
 */
class ValueModelModuleTest {

    // ---------- VersionedValue ----------

    @Test
    void versionedValue_rejectsNegativeVersion() {
        assertThrows(IllegalArgumentException.class, () -> new VersionedValue<>("v", -1L));
        assertDoesNotThrow(() -> new VersionedValue<>("v", 0L));
        assertDoesNotThrow(() -> new VersionedValue<>("v", 10L));
    }

    @Test
    void versionedValue_equalsAndHashCode() {
        VersionedValue<String> a = new VersionedValue<>("v", 1L);
        VersionedValue<String> b = new VersionedValue<>("v", 1L);
        VersionedValue<String> c = new VersionedValue<>("v", 2L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    // ---------- CacheEntry ----------

    @Test
    void cacheEntry_fromNoTtl_neverExpires() {
        CacheEntry<String> entry = CacheEntry.from(new VersionedValue<>("v", 1L));
        assertFalse(entry.nullValue());
        assertEquals(1L, entry.version());
        assertFalse(entry.isPhysicalExpired(System.currentTimeMillis() + 100_000L));
    }

    @Test
    void cacheEntry_fromWithTtl_setsPhysicalExpiry() {
        CacheEntry<String> entry = CacheEntry.from(new VersionedValue<>("v", 1L), Duration.ofSeconds(30));
        assertFalse(entry.isPhysicalExpired(System.currentTimeMillis()));
        assertTrue(entry.isPhysicalExpired(System.currentTimeMillis() + 31_000L));
    }

    @Test
    void cacheEntry_fromNullValue_marksNull() {
        CacheEntry<String> entry = CacheEntry.from(new VersionedValue<>(null, 5L), Duration.ofMinutes(1));
        assertTrue(entry.nullValue());
        assertNull(entry.value());
        assertEquals(5L, entry.version());
    }

    @Test
    void cacheEntry_empty_rejectsInvalidTtl() {
        assertThrows(IllegalArgumentException.class, () -> CacheEntry.empty(0L, null));
        assertThrows(IllegalArgumentException.class, () -> CacheEntry.empty(0L, Duration.ZERO));
    }

    @Test
    void cacheEntry_empty_createsNullPlaceholderWithTtl() {
        CacheEntry<String> empty = CacheEntry.empty(0L, Duration.ofMinutes(1));
        assertTrue(empty.nullValue());
        assertNull(empty.value());
        assertFalse(empty.isPhysicalExpired(System.currentTimeMillis()));
        assertTrue(empty.isPhysicalExpired(System.currentTimeMillis() + 61_000L));
    }

    @Test
    void cacheEntry_logicalAndPhysicalExpiry_independent() {
        CacheEntry<String> entry = new CacheEntry<>("v", 1L, false,
                System.currentTimeMillis() - 1, 0L);
        assertTrue(entry.isLogicalExpired(System.currentTimeMillis()));
        assertFalse(entry.isPhysicalExpired(System.currentTimeMillis()), "物理过期时间<=0 表示永不过期");
    }

    // ---------- CacheInvalidationEvent ----------

    @Test
    void event_rejectsBlankFields() {
        assertThrows(IllegalArgumentException.class,
                () -> new CacheInvalidationEvent(null, "k", 1L, EventType.INVALIDATE, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new CacheInvalidationEvent("", "k", 1L, EventType.INVALIDATE, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new CacheInvalidationEvent("e1", "", 1L, EventType.INVALIDATE, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new CacheInvalidationEvent("e1", "k", 1L, null, 0L));
    }

    @Test
    void event_equalsBasedOnAllFields() {
        long now = System.currentTimeMillis();
        CacheInvalidationEvent a = new CacheInvalidationEvent("e1", "k", 1L, EventType.INVALIDATE, now);
        CacheInvalidationEvent b = new CacheInvalidationEvent("e1", "k", 1L, EventType.INVALIDATE, now);
        CacheInvalidationEvent c = new CacheInvalidationEvent("e1", "k", 2L, EventType.INVALIDATE, now);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}

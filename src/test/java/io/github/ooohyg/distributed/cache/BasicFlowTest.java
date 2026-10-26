package io.github.ooohyg.distributed.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景一：基本读链路与回填验证（L3 -> L2 -> L1 逐级回填）。
 */
class BasicFlowTest extends BaseCacheTest {

    @Test
    @DisplayName("读未命中：穿透到 L3 并回填 L2 和 L1")
    void testReadThroughAndFill() {
        // 1. 往数据源写入数据（模拟 DB 插入）
        source.put("user:1", "Alice");

        // 2. 第一次查（全未命中，查 DB）
        Optional<String> result = cache.get("user:1");

        // 3. 断言
        assertEquals("Alice", result.orElse(null));
        assertEquals(1, source.readCount(), "应该查了 1 次数据库");
        assertTrue(l1.getEntry("user:1").isPresent(), "L1 应该被回填");
        assertTrue(l2.getEntry("user:1").isPresent(), "L2 应该被回填");
        assertEquals(1L, l1.getEntry("user:1").orElseThrow().version(), "回填版本应来自数据源");
        assertEquals(1, metrics.missL1Count(), "L1 未命中应记 1 次");
        assertEquals(1, metrics.missL2Count(), "L2 未命中应记 1 次");

        // 4. 第二次查（应该命中 L1，不查 DB）
        source.resetReadCount();
        Optional<String> second = cache.get("user:1");
        assertEquals("Alice", second.orElse(null));
        assertEquals(0, source.readCount(), "命中缓存，不应该查 DB");
        assertEquals(1, metrics.hitL1Count(), "第二次读取应命中 L1");
    }

    @Test
    @DisplayName("L1 未命中、L2 命中：回填 L1 且不穿透到 L3")
    void testL2HitBackfillsL1WithoutSourceRead() {
        source.put("user:2", "Bob");
        // 第一次读，三层都走到
        cache.get("user:2");
        assertEquals(1, source.readCount());

        // 只清 L1，保留 L2（evict 只作用于 L1）
        l1.evict("user:2");
        source.resetReadCount();

        Optional<String> result = cache.get("user:2");
        assertEquals("Bob", result.orElse(null));
        assertEquals(0, source.readCount(), "L2 命中不应回源");
        assertTrue(l1.getEntry("user:2").isPresent(), "L2 命中应回填 L1");
        assertEquals(1, metrics.hitL2Count(), "应记一次 L2 命中");
    }
}

package io.github.ooohyg.distributed.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景三：空值缓存防穿透验证。
 */
class PenetrationTest extends BaseCacheTest {

    @Test
    @DisplayName("缓存穿透防护：空值占位符生效")
    void testNullValueCache() {
        // 不往数据源写数据，直接查
        Optional<String> result1 = cache.get("ghost-key");
        assertTrue(result1.isEmpty(), "查不到数据应该返回 empty");
        assertEquals(1, source.readCount(), "第一次应该穿透到 DB");

        // 验证 L1 中是否有空值占位符（nullValue=true）
        Optional<CacheEntry<String>> l1Entry = l1.getEntry("ghost-key");
        assertTrue(l1Entry.isPresent(), "应该写入了空值占位符");
        assertTrue(l1Entry.orElseThrow().nullValue(), "标记必须是空值");
        assertTrue(l2.getEntry("ghost-key").isPresent(), "L2 也应写入空值占位符");

        // 第二次查同一个不存在的 key
        source.resetReadCount();
        Optional<String> result2 = cache.get("ghost-key");
        assertTrue(result2.isEmpty(), "空值占位符命中，仍返回 empty");
        assertEquals(0, source.readCount(), "第二次不应穿透到 DB，应直接命中空值");
        assertEquals(1, metrics.nullHitCount(), "应记一次空值命中");
    }

    @Test
    @DisplayName("空值占位符被清后，L2 中的空值仍能挡住穿透")
    void testNullPlaceholderInL2BlocksPenetration() {
        cache.get("ghost-key-2");
        assertEquals(1, source.readCount());

        // 清掉 L1，只留 L2 的空值占位符
        l1.evict("ghost-key-2");
        source.resetReadCount();

        Optional<String> result = cache.get("ghost-key-2");
        assertTrue(result.isEmpty(), "L2 空值占位符命中仍返回 empty");
        assertEquals(0, source.readCount(), "不应再次穿透到 DB");
    }
}

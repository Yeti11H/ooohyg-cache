package io.github.ooohyg.distributed.cache.protection;

/**
 * 防缓存穿透。
 *
 * <p>缓存穿透：查询一个根本不存在的 key，每次都穿到 L3 数据源。
 * 黑客用大量不存在的 key 攻击时，会打挂 DB。
 *
 * <p>本接口只抽象布隆过滤器——空值缓存不在这里实现，
 * 它在读策略里通过 CacheEntry.nullValue 表达。
 *
 * <p>方法语义：
 * <ul>
 *   <li>{@code mightExist} 返回 false → 肯定不存在，直接返回 empty，不查 L3；</li>
 *   <li>{@code mightExist} 返回 true  → 可能存在（可能是假阳性），放行查 L3。</li>
 * </ul>
 *
 * @param <K> key 类型
 */
public interface CachePenetrationGuard<K> {

    /**
     * 查询 key 是否可能存在。
     *
     * @return false 表示肯定不存在；true 表示可能存在（可能误判）
     */
    boolean mightExist(K key);

    /**
     * 标记 key 存在。数据源写入新数据时调用。
     */
    void markExists(K key);
}
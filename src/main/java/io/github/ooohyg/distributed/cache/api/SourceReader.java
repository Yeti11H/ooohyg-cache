package io.github.ooohyg.distributed.cache.api;

import io.github.ooohyg.distributed.cache.VersionedValue;

/**
 * 数据源读取器。SourceWriter 的读侧对偶——一个读数据源，一个写数据源。
 *
 * <p>命名说明：不叫 CacheLoader，因为它加载的不是缓存，是数据源。
 * 和 SourceWriter 成对，采用 Reader/Writer 的经典对偶命名。
 *
 * <p>职责：从 L3 真实数据源读取一个值，连同版本号返回。
 *
 * <p>返回值语义：
 *  - 返回 VersionedValue(value, version) → 数据存在
 *  - 返回 null                           → 数据源里确实没有这个 key（正常业务结果）
 *  - 抛出 Exception                      → 下游故障（超时、连接失败、业务异常）
 *
 * <p>为什么 load 抛 Exception 而不是 RuntimeException：
 * 强制调用方显式处理失败。loader 可能超时、可能连接失败、可能抛业务异常，
 * 这些都不是"意外"，而是必须处理的正常路径。
 *
 * <p>为什么不用 Optional&lt;VersionedValue&gt; 表示"不存在"：
 * 因为要区分三种情况——存在 / 不存在 / 故障。
 * Optional 只能表达两种。用 null 表示"不存在"，用 Exception 表示"故障"。
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
@FunctionalInterface
public interface SourceReader<K, V> {

    /**
     * 从数据源读取。
     *
     * @param key 缓存 key
     * @return VersionedValue(value, version)；返回 null 表示数据不存在
     * @throws Exception 下游故障
     */
    VersionedValue<V> read(K key) throws Exception;
}
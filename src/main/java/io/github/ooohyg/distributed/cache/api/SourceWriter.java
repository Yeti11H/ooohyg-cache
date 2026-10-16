package io.github.ooohyg.distributed.cache.api;

import io.github.ooohyg.distributed.cache.VersionedValue;

/**
 * 数据源写入器。SourceReader 的写侧对偶——一个读数据源，一个写数据源。
 *
 * <p>命名说明：和 SourceReader 成对，采用 Reader/Writer 的经典对偶命名。
 *
 * <p>职责：更新 L3 真实数据源，返回带新版本号的值。
 *
 * <p>为什么输入是裸值 V、输出是 VersionedValue：
 * 业务代码调 cache.put(key, value) 时并不知道版本号——
 * 版本号是数据源内部管理的（如数据库的 update_version 字段）。
 * SourceWriter 的实现负责"写完之后拿到新版本号"。
 *
 * <p>为什么返回 VersionedValue 而不是 void：
 * 版本号要传给失效广播事件。接收方收到事件后，
 * 用 version 判断"这个事件是不是比本地缓存的版本更新"，
 * 决定是否要删本地缓存。
 *
 * <p>两个接口的返回类型完全一致（都是 VersionedValue）——这是对称设计的必然：
 * 不管是"读到的值"还是"刚写进去的值"，都需要携带版本号，
 * 才能参与后续的一致性决策。
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
@FunctionalInterface
public interface SourceWriter<K, V> {

    /**
     * 写入数据源，返回带新版本号的值。
     *
     * @param key   写入的 key
     * @param value 写入的值（裸值，不含版本）
     * @return 写入后的 VersionedValue（value + 递增后的 version）
     * @throws Exception 数据源写入失败
     */
    VersionedValue<V> write(K key, V value) throws Exception;
}
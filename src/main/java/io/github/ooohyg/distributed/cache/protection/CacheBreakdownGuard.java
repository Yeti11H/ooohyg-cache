package io.github.ooohyg.distributed.cache.protection;

import java.time.Duration;

/**
 * 防缓存击穿。
 *
 * <p>缓存击穿：热点 key 过期瞬间，大量请求同时回源。
 * 单 JVM 内已由 SingleFlight 合并；跨 JVM 由本接口提供互斥。
 *
 * <p>本接口只做"抢占式一次性锁"——够防击穿即可，不做通用分布式锁
 * （不做可重入、不做公平锁、不做读写锁）。通用分布式锁请用 Redisson。
 *
 * <p>用法：
 * <pre>{@code
 * try (LockHandle h = guard.lock(key, Duration.ofSeconds(10))) {
 *     if (h.isAcquired()) {
 *         // 我抢到了锁，回源
 *     } else {
 *         // 别人在回源，等一会儿再读缓存
 *     }
 * }
 * }</pre>
 *
 * @param <K> key 类型
 */
public interface CacheBreakdownGuard<K> {

    /**
     * 尝试获取锁。返回的句柄支持 try-with-resources 自动释放。
     */
    LockHandle lock(K key, Duration ttl);

    /**
     * 锁句柄。{@link #close()} 时精确释放自己的锁，不会误删别人的。
     */
    interface LockHandle extends AutoCloseable {

        /**
         * 是否成功拿到锁。
         */
        boolean isAcquired();

        /**
         * 释放锁。未拿到锁时调用应无副作用。
         */
        @Override
        void close();
    }
}
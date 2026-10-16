package io.github.ooohyg.distributed.cache.core;

import io.github.ooohyg.distributed.cache.VersionedValue;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 并发回源合并器。同一 key 的多个并发请求，只有一个真正执行 loader，
 * 其他请求等待第一个请求的结果。
 *
 * <p>设计要点：
 * <ul>
 *   <li>不同 key 不互相阻塞——ConcurrentHashMap 按 key 粒度隔离；</li>
 *   <li>loader 失败时，等待者通过 completeExceptionally 拿到原始异常；</li>
 *   <li>等待者设超时——防止 loader 卡死拖垮所有等待线程；</li>
 *   <li>finally 里 remove(key, future)——防止 Map 无限增长（内存泄漏）。</li>
 * </ul>
 *
 * <p>注意：只合并"同一 key 的并发回源"，不做线程池——真正的 loader 调用
 * 在第一个到达者的线程上执行，不切换线程。
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
public class SingleFlight<K, V> {

    private final ConcurrentHashMap<K, CompletableFuture<VersionedValue<V>>> inFlight =
            new ConcurrentHashMap<>();

    private final Duration waitTimeout;
    private final AtomicLong mergeCount = new AtomicLong(0);

    public SingleFlight() {
        this(Duration.ofSeconds(5));
    }

    public SingleFlight(Duration waitTimeout) {
        if (waitTimeout.isZero() || waitTimeout.isNegative()) {
            throw new IllegalArgumentException("waitTimeout must be positive");
        }
        this.waitTimeout = waitTimeout;
    }

    /**
     * 执行 loader，合并同一 key 的并发调用。
     *
     * @param key    key
     * @param loader 真正回源的逻辑
     * @return loader 的结果
     * @throws Exception loader 抛出的原始异常
     */
    public VersionedValue<V> execute(K key, Callable<VersionedValue<V>> loader) throws Exception {
        CompletableFuture<VersionedValue<V>> myFuture = new CompletableFuture<>();
        CompletableFuture<VersionedValue<V>> existing = inFlight.putIfAbsent(key, myFuture);

        if (existing != null) {
            // 已有线程在回源：等待它的结果（合并计数）
            mergeCount.incrementAndGet();
            return awaitResult(key, existing);
        }

        // 自己是第一个：真正执行 loader
        try {
            VersionedValue<V> result = loader.call();
            myFuture.complete(result);
            return result;
        } catch (Exception e) {
            // 让等待者也能感知到失败
            myFuture.completeExceptionally(e);
            throw e;
        } finally {
            // 清理：注意用 (key, myFuture) 双参数，防止误删新放入的 future
            inFlight.remove(key, myFuture);
        }
    }

    private VersionedValue<V> awaitResult(K key, CompletableFuture<VersionedValue<V>> future)
            throws Exception {
        try {
            return future.get(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            // 还原成 loader 的原始异常
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException("loader failed for key=" + key, cause);
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    "singleflight wait timeout for key=" + key + " after " + waitTimeout, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while waiting for key=" + key, e);
        }
    }

    /**
     * 被合并的请求数（用于指标）。
     */
    public long mergeCount() {
        return mergeCount.get();
    }

    /**
     * 当前正在回源的 key 数（用于调试和监控）。
     */
    public int inFlightCount() {
        return inFlight.size();
    }
}
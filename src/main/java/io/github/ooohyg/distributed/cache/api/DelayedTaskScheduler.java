package io.github.ooohyg.distributed.cache.api;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 延迟任务调度器（带 debounce）。
 *
 * <p>中性工具——不绑定任何业务语义。当前用于：
 * <ul>
 *   <li>延迟双删的第二次删；</li>
 *   <li>（后续）逻辑过期的后台异步刷新；</li>
 *   <li>（后续）失败重试。</li>
 * </ul>
 *
 * <p>debounce：同一 key 的多次 schedule 会取消前一个任务，只保留最后一个。
 * 效果：连续 schedule 同一个 key 时，只在最后一次之后 N 毫秒执行一次。
 *
 * <p>为什么用 ScheduledExecutorService 而不是 MQ：
 * 教学版单实例够用。生产级应换成 Redis ZSet（score=到期时间戳）或 MQ 延迟消息，
 * 支持持久化和跨实例。JVM 重启会丢任务——调用方需自行评估是否能接受。
 *
 * <p>线程模型：单线程调度池。任务执行应快速返回；
 * 如果任务本身耗时长，会阻塞后续任务的调度。
 *
 * @param <K> debounce 的 key 类型
 */
public class DelayedTaskScheduler<K> {

    private final ScheduledExecutorService scheduler;
    private final Duration delay;
    private final ConcurrentHashMap<K, ScheduledFuture<?>> pending = new ConcurrentHashMap<>();

    public DelayedTaskScheduler(Duration delay) {
        if (delay == null || delay.isZero() || delay.isNegative()) {
            throw new IllegalArgumentException("delay must be positive");
        }
        this.delay = delay;
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "hyg-cache-delayed-task");
            t.setDaemon(true);   // 不阻塞 JVM 退出
            return t;
        });
    }

    /**
     * 安排一次延迟任务。同一 key 的旧任务会被取消（debounce）。
     *
     * @param key  debounce 的 key
     * @param task 延迟到期后要执行的任务
     */
    public void schedule(K key, Runnable task) {
        AtomicReference<ScheduledFuture<?>> selfRef = new AtomicReference<>();

        ScheduledFuture<?> newFuture = scheduler.schedule(() -> {
            try {
                task.run();
            } finally {
                // 任务执行完后从 Map 移除自己（只移除自己那个 future）
                ScheduledFuture<?> self = selfRef.get();
                if (self != null) {
                    pending.remove(key, self);
                }
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);

        selfRef.set(newFuture);

        ScheduledFuture<?> old = pending.put(key, newFuture);
        if (old != null) {
            // cancel(false) 不中断正在执行的任务——
            // 如果旧任务已经跑到一半，多删一次缓存是无害的（幂等）
            old.cancel(false);
        }
    }

    /**
     * 当前待执行的任务数（用于测试断言和监控）。
     */
    public int pendingCount() {
        return pending.size();
    }

    /**
     * 关闭调度器。等待中但还没执行的任务会被取消。
     */
    public void shutdown() {
        scheduler.shutdownNow();
        pending.clear();
    }
}
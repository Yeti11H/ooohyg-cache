package io.github.ooohyg.distributed.cache.invalidation;

import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 可靠事件发布器（Outbox 模式）。
 * 业务先落库，后台线程异步发送，失败重试，保证最终发出。
 */
public class OutboxInvalidationPublisher implements CacheInvalidationPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxInvalidationPublisher.class);

    private final CacheInvalidationPublisher delegate;
    // 模拟 Outbox 表：key=eventId, value=事件
    private final Map<String, CacheInvalidationEvent> outbox = new ConcurrentHashMap<>();
    private final ExecutorService relayThread = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "outbox-relay");
        t.setDaemon(true);
        return t;
    });

    public OutboxInvalidationPublisher(CacheInvalidationPublisher delegate) {
        this.delegate = delegate;
        startRelay();
    }

    @Override
    public void publish(CacheInvalidationEvent event) {
        // 核心改变：不直接发送，而是先写入 Outbox 表
        outbox.put(event.eventId(), event);
    }

    private void startRelay() {
        relayThread.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(100); // 模拟定时扫表
                    for (Map.Entry<String, CacheInvalidationEvent> entry : outbox.entrySet()) {
                        try {
                            delegate.publish(entry.getValue()); // 真正调用 Redis Pub/Sub
                            outbox.remove(entry.getKey());      // 发送成功，删除记录
                        } catch (Exception e) {
                            // 发送失败：保留在 Outbox 中，下次循环重试
                            log.warn("Outbox relay failed, will retry: eventId={}", entry.getKey(), e);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
}
package io.github.ooohyg.distributed.cache;

/**
 * 失效事件类型。
 *
 * <p>第一版只使用 {@link #INVALIDATE}（失效广播）；
 * {@link #PUT} 是后续“广播写”才需要的语义。
 */
public enum EventType {
    INVALIDATE,
    PUT
}

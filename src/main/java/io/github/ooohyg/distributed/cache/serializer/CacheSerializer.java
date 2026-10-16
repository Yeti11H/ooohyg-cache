package io.github.ooohyg.distributed.cache.serializer;

/**
 * 缓存值序列化器。第一版只定义接口不实现（内存链路不需要序列化），
 * 后续接 Redis L2 时再提供 Jackson 等实现。
 *
 * @param <V> value 类型
 */
public interface CacheSerializer<V> {

    /**
     * 序列化为字节数组。
     */
    byte[] serialize(V value);

    /**
     * 反序列化。
     */
    V deserialize(byte[] bytes);
}

package io.github.ooohyg.distributed.cache.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.serializer.CacheSerializer;

import java.io.IOException;

/**
 * 基于 Jackson 的缓存条目序列化器。
 *
 * <p>为什么需要 Class 参数：泛型擦除导致反序列化时
 * Jackson 不知道 CacheEntry 里的 value 是什么类型。
 *
 * @param <V> value 类型（不是 CacheEntry<V>，是里面的 V）
 */
public class JacksonCacheSerializer<V> implements CacheSerializer<CacheEntry<V>> {

    private final ObjectMapper mapper;
    private final JavaType entryType;

    public JacksonCacheSerializer(ObjectMapper mapper, Class<V> valueClass) {
        this.mapper = mapper;
        this.entryType = mapper.getTypeFactory()
                .constructParametricType(CacheEntry.class, valueClass);
    }

    @Override
    public byte[] serialize(CacheEntry<V> entry) {
        try {
            return mapper.writeValueAsBytes(entry);
        } catch (JsonProcessingException e) {
            throw new SerializationException("serialize failed", e);
        }
    }

    @Override
    public CacheEntry<V> deserialize(byte[] bytes) {
        try {
            return mapper.readValue(bytes, entryType);
        } catch (IOException e) {
            throw new SerializationException("deserialize failed", e);
        }
    }
}
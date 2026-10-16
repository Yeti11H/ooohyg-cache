package io.github.ooohyg.distributed.cache.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;

import java.io.IOException;

/**
 * 失效事件的 Jackson 序列化器。
 *
 * <p>为什么不需要传 Class：
 * CacheInvalidationEvent 不是泛型类——它的每个字段类型都是固定的
 * （String、long、枚举），Jackson 直接知道怎么解析。
 * 这是和 JacksonCacheSerializer 的核心区别。
 */
public class JacksonEventSerializer implements CacheSerializer<CacheInvalidationEvent> {

    private final ObjectMapper mapper;

    public JacksonEventSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] serialize(CacheInvalidationEvent event) {
        try {
            return mapper.writeValueAsBytes(event);
        } catch (IOException e) {
            throw new SerializationException("serialize event failed", e);
        }
    }

    @Override
    public CacheInvalidationEvent deserialize(byte[] bytes) {
        try {
            return mapper.readValue(bytes, CacheInvalidationEvent.class);
        } catch (IOException e) {
            throw new SerializationException("deserialize event failed", e);
        }
    }
}
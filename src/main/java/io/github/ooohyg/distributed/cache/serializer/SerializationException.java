package io.github.ooohyg.distributed.cache.serializer;

/**
 * 序列化/反序列化失败。
 */
public class SerializationException extends RuntimeException {

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
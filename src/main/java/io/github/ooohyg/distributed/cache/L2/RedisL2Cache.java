package io.github.ooohyg.distributed.cache.L2;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.api.CacheLevel;
import io.github.ooohyg.distributed.cache.serializer.CacheSerializer;
import io.github.ooohyg.distributed.cache.serializer.CacheSerializer;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

/**
 * 基于 Redis（Lettuce）的 L2 缓存。
 *
 * <p>Redis EXPIRE = 业务 TTL 剩余时间 + 缓冲（默认 1 分钟）。
 * 缓冲的作用：给失效广播留传播窗口，防止 Redis 先删 key 导致版本信息丢失。
 *
 * <p>若 {@code CacheEntry.physicalExpireAtMillis <= 0}（业务未设 TTL），
 * 使用 {@code fallbackTtl}（默认 24 小时），防止 key 永久占用 Redis 内存。
 *
 * <p>本类不做异常处理——Redis 连接失败会抛 RedisConnectionException，
 * 由上层（MultiLevelCache / FailurePolicy）决定降级策略。
 */
public class RedisL2Cache<K, V> implements CacheLevel<K, V> {

    private static final Duration DEFAULT_EXPIRE_BUFFER = Duration.ofMinutes(1);
    private static final Duration DEFAULT_FALLBACK_TTL = Duration.ofHours(24);

    private final RedisCommands<String, byte[]> commands;
    private final CacheSerializer<CacheEntry<V>> serializer;
    private final String keyPrefix;
    private final Function<K, String> keyToString;
    private final Duration expireBuffer;
    private final Duration fallbackTtl;
    private final String name;

    public RedisL2Cache(RedisCommands<String, byte[]> commands,
                        CacheSerializer<CacheEntry<V>> serializer,
                        String keyPrefix,
                        Function<K, String> keyToString,
                        String name) {
        this(commands, serializer, keyPrefix, keyToString, name,
                DEFAULT_EXPIRE_BUFFER, DEFAULT_FALLBACK_TTL);
    }

    public RedisL2Cache(RedisCommands<String, byte[]> commands,
                        CacheSerializer<CacheEntry<V>> serializer,
                        String keyPrefix,
                        Function<K, String> keyToString,
                        String name,
                        Duration expireBuffer,
                        Duration fallbackTtl) {
        this.commands = commands;
        this.serializer = serializer;
        this.keyPrefix = keyPrefix;
        this.keyToString = keyToString;
        this.name = name;
        this.expireBuffer = expireBuffer;
        this.fallbackTtl = fallbackTtl;
    }

    @Override
    public Optional<CacheEntry<V>> getEntry(K key) {
        String fullKey = fullKey(key);
        byte[] bytes = commands.get(fullKey);
        if (bytes == null) {
            return Optional.empty();
        }
        CacheEntry<V> entry = serializer.deserialize(bytes);
        // 惰性删除：Redis EXPIRE 和 CacheEntry 过期时间之间有时间差
        if (entry.isPhysicalExpired(System.currentTimeMillis())) {
            commands.del(fullKey);
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    @Override
    public void putEntry(K key, CacheEntry<V> entry) {
        String fullKey = fullKey(key);
        byte[] bytes = serializer.serialize(entry);
        long redisTtlMillis = computeRedisTtl(entry, System.currentTimeMillis());
        commands.set(fullKey, bytes);
        commands.pexpire(fullKey, redisTtlMillis);
    }

    private long computeRedisTtl(CacheEntry<V> entry, long now) {
        long physicalExpireAt = entry.physicalExpireAtMillis();
        long businessTtl;
        if (physicalExpireAt <= 0) {
            businessTtl = fallbackTtl.toMillis();
        } else {
            businessTtl = physicalExpireAt - now;
            if (businessTtl <= 0) {
                businessTtl = 1L;   // 已过期：给最小 TTL，让它尽快消失
            }
        }
        return businessTtl + expireBuffer.toMillis();
    }

    @Override
    public void evict(K key) {
        commands.del(fullKey(key));
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(
                "clear() is not supported on Redis L2; use evict(key) instead");
    }

    @Override
    public String name() {
        return name;
    }

    private String fullKey(K key) {
        return keyPrefix + keyToString.apply(key);
    }
}
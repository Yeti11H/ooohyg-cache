package io.github.ooohyg.distributed.cache.failure;

import java.util.Optional;

/**
 * 失败降级策略。MultiLevelCache 在两类失败场景下调用它：
 *  1. L3 CacheLoader 抛异常时
 *  2. 某一层缓存（L1/L2）读写抛异常时
 *
 * <p>返回 Optional.empty() 表示"无法给出结果，向上传递失败"；
 * 返回 Optional.of(value) 表示"用这个值作为降级结果"。
 *
 * <p>为什么返回 Optional 而不是抛异常：
 * 降级决策应该在编排层做，而不是让异常穿透到业务代码。
 * 业务代码看到的是 Optional.empty()，而不是 RedisConnectionException。
 *
 * <p>为什么两个方法而不是一个：
 * "L3 加载失败"和"某层缓存失败"的降级策略不同。
 * L3 失败时业务通常需要一个值（哪怕是旧值）；
 * 某层缓存失败时通常只需要跳过这层，继续往下查。
 * 用字符串"L3"/"L2"判断类型是脆弱的——拼错不报错。
 * 两个方法在类型层面区分开，编译器帮你检查。
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
public interface FailurePolicy<K, V> {

    /**
     * L3 加载失败时调用。
     *
     * @param key   失败的 key
     * @param error L3 抛出的异常
     * @return 降级结果；Optional.empty() 表示拒答，业务看到"没有值"
     */
    Optional<V> onLoadFailure(K key, Throwable error);

    /**
     * 某一层缓存（L1/L2）读写失败时调用。
     *
     * @param key       失败的 key
     * @param levelName 层名称，如 "InMemoryL1Cache"、"RedisL2Cache"
     * @param error     抛出的异常
     * @return 降级结果；Optional.empty() 表示"忽略这一层，继续往下走"
     */
    Optional<V> onLevelFailure(K key, String levelName, Throwable error);
}
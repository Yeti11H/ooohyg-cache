package io.github.ooohyg.distributed.cache.protection;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 基于随机抖动的防雪崩实现。
 *
 * <p>给基础 TTL 加一个随机偏移，让 key 的过期时间打散。
 * 效果：本来同一时刻到期的 N 个 key，被打散到一段窗口内。
 *
 * <p>默认抖动比例 ±10%——即 30 分钟的 TTL 会被抖到 [27m, 33m]。
 * 这个比例是经验值：太小（<5%）打散效果不明显；太大（>30%）会让
 * 平均缓存时间偏离业务预期。
 *
 * <p>为什么用 ThreadLocalRandom 而不是 Random：
 * ThreadLocalRandom 在高并发下没有竞争，性能是 Random 的数倍。
 *
 * @param <K> key 类型
 */
public class JitterTtlAvalancheGuard<K> implements CacheAvalancheGuard<K> {

    private static final double DEFAULT_JITTER_RATIO = 0.1;

    private final double jitterRatio;

    public JitterTtlAvalancheGuard() {
        this(DEFAULT_JITTER_RATIO);
    }

    public JitterTtlAvalancheGuard(double jitterRatio) {
        if (jitterRatio < 0 || jitterRatio >= 1) {
            throw new IllegalArgumentException("jitterRatio must be in [0, 1)");
        }
        this.jitterRatio = jitterRatio;
    }

    @Override
    public Duration computeTtl(K key, Duration baseTtl) {
        if (baseTtl == null || baseTtl.isNegative() || baseTtl.isZero()) {
            throw new IllegalArgumentException("baseTtl must be positive");
        }
        long base = baseTtl.toMillis();
        long jitterRange = (long) (base * jitterRatio);
        // 抖动区间：[base - jitterRange, base + jitterRange]
        long jitter = ThreadLocalRandom.current().nextLong(-jitterRange, jitterRange + 1);
        long finalTtl = base + jitter;
        // 防御：抖动后不能为 0 或负数
        if (finalTtl <= 0) {
            finalTtl = 1;
        }
        return Duration.ofMillis(finalTtl);
    }
}
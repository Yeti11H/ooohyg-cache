package io.github.ooohyg.distributed.cache.protection;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;

/**
 * 基于 BitSet 的内存布隆过滤器。
 *
 * <p>原理：
 * <ul>
 *   <li>维护一个 m 位的 BitSet；</li>
 *   <li>用 k 个哈希函数计算 key 的 k 个位置，全部设为 1；</li>
 *   <li>查询时，k 个位置全部为 1 → 可能存在；有任何一个为 0 → 肯定不存在。</li>
 * </ul>
 *
 * <p>重要特性（面试必答）：
 * <ul>
 *   <li>可能假阳性（False Positive）：不存在被判为存在；</li>
 *   <li>绝不假阴性（False Negative）：存在不会判为不存在；</li>
 *   <li>不能删除元素——删一个元素需要清位，但会误伤其他 key 的位。</li>
 * </ul>
 *
 * <p>参数：
 * <ul>
 *   <li>expectedInsertions = 100 万，误判率 1% → m ≈ 9.6 Mbit ≈ 1.2 MB；</li>
 *   <li>误判率越低，m 越大——需要业务权衡。</li>
 * </ul>
 *
 * <p>第一版是内存版——单实例。跨实例需要用 Redis 位图
 * （RedisBloom 模块或手写 SETBIT）。生产环境可用 Redisson 的 RBloomFilter。
 *
 * @param <K> key 类型
 */
public class BloomFilterPenetrationGuard<K> implements CachePenetrationGuard<K> {

    private final BitSet bits;
    private final int bitSize;
    private final int hashCount;

    public BloomFilterPenetrationGuard(int expectedInsertions, double falsePositiveRate) {
        this.bitSize = optimalBitSize(expectedInsertions, falsePositiveRate);
        this.hashCount = optimalHashCount(expectedInsertions, bitSize);
        this.bits = new BitSet(bitSize);
    }

    @Override
    public boolean mightExist(K key) {
        long[] hashes = hashes(key);
        for (long hash : hashes) {
            int index = (int) (Math.floorMod(hash, bitSize));
            if (!bits.get(index)) {
                return false;   // 有一个位置是 0 → 肯定不存在
            }
        }
        return true;   // 全部为 1 → 可能存在
    }

    @Override
    public void markExists(K key) {
        long[] hashes = hashes(key);
        for (long hash : hashes) {
            int index = (int) (Math.floorMod(hash, bitSize));
            bits.set(index);
        }
    }

    /** 当前已设置的位数（用于测试和监控）。 */
    public int cardinality() {
        return bits.cardinality();
    }

    // ---- 内部：布隆过滤器的标准公式 ----

    private static int optimalBitSize(int n, double p) {
        // m = -n * ln(p) / (ln2)^2
        return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalHashCount(int n, int m) {
        // k = m / n * ln2
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * 用同一个 key 生成 k 个哈希值。
     * 第一版用简单双重哈希——生产可用 MurmurHash 或 Guava 的 Hashing。
     */
    private long[] hashes(K key) {
        byte[] bytes = String.valueOf(key).getBytes(StandardCharsets.UTF_8);
        long h1 = fnv1a(bytes, 0);
        long h2 = fnv1a(bytes, h1);
        long[] result = new long[hashCount];
        for (int i = 0; i < hashCount; i++) {
            // 双重哈希：h_i = h1 + i * h2
            result[i] = h1 + i * h2;
        }
        return result;
    }

    /** FNV-1a 哈希，简单快速，够用。 */
    private static long fnv1a(byte[] bytes, long seed) {
        long hash = 0xcbf29ce484222325L ^ seed;
        for (byte b : bytes) {
            hash ^= (b & 0xff);
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
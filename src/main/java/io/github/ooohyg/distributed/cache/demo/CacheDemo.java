package io.github.ooohyg.distributed.cache.demo;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.EventType;
import io.github.ooohyg.distributed.cache.L1.ConcurrentHashMapL1Cache;
import io.github.ooohyg.distributed.cache.L2.InMemoryL2Cache;
import io.github.ooohyg.distributed.cache.VersionedValue;
import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import io.github.ooohyg.distributed.cache.api.CacheLevel;
import io.github.ooohyg.distributed.cache.api.SourceReader;
import io.github.ooohyg.distributed.cache.api.SourceWriter;
import io.github.ooohyg.distributed.cache.core.MultiLevelCache;
import io.github.ooohyg.distributed.cache.core.SingleFlight;
import io.github.ooohyg.distributed.cache.core.strategy.read.CacheAsideReadStrategy;
import io.github.ooohyg.distributed.cache.core.strategy.write.CacheAsideWriteStrategy;
import io.github.ooohyg.distributed.cache.invalidation.InMemoryInvalidationBus;
import io.github.ooohyg.distributed.cache.metrics.CacheMetrics;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可运行的纯内存演示程序。
 *
 * <p>不依赖 Redis、不依赖 Spring、不需要 Docker：L1 使用
 * {@link ConcurrentHashMapL1Cache}，L2 使用 {@link InMemoryL2Cache}，
 * 失效广播使用 {@link InMemoryInvalidationBus}，数据源是与源码同构的内存实现
 * （{@link SourceReader} + {@link SourceWriter} 两个函数式接口，生产环境换成
 * 自己的 DAO / RPC 即可）。
 *
 * <p>演示覆盖 6 个场景：
 * <ol>
 *   <li>首次读取穿透回源并回填 L1/L2，二次读取命中 L1；</li>
 *   <li>多线程并发读取同一 Key，由 SingleFlight 合并，只回源 1 次；</li>
 *   <li>数据源明确返回“不存在”时写入空值占位，后续请求不再穿透；</li>
 *   <li>写操作更新数据源后清除本地缓存并广播失效事件，其他实例同步失效；</li>
 *   <li>晚到的旧版本失效事件被丢弃，不会误删新数据；</li>
 *   <li>数据源故障时通过旧值兜底，业务仍能拿到可用结果。</li>
 * </ol>
 *
 * <p>运行方式见项目根目录 README 的 “可运行演示” 一节。
 */
public final class CacheDemo {

    private static final String HEAVY_LINE = "================================================================================";
    private static final String THIN_LINE = "--------------------------------------------------------------------------------";

    /** 每个场景的汇总信息，最后统一打印成表。 */
    private record ScenarioResult(String index,
                                  String name,
                                  String outcome,
                                  int sourceReads,
                                  String hitRate) {
    }

    private final List<ScenarioResult> results = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        // 固定以 UTF-8 输出：mvn exec:java 与 IDE 控制台时中文不会乱码
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));

        CacheDemo demo = new CacheDemo();
        demo.printBanner();
        demo.scenario1ColdReadAndL1Hit();
        demo.scenario2SingleFlight();
        demo.scenario3NullPlaceholder();
        demo.scenario4WriteInvalidation();
        demo.scenario5StaleEventDiscarded();
        demo.scenario6FailureFallbackToStale();
        demo.printSummary();
    }

    // ---------------------------------------------------------------- 场景 1

    /**
     * 场景 1：首次读取穿透回源并回填 L1/L2，二次读取命中 L1。
     */
    private void scenario1ColdReadAndL1Hit() {
        printTitle("场景 1 / 6", "冷读穿透回源 + 逐层回填，二次读取命中 L1");
        Deployment d = deploy();
        d.source.put("user:1", "Alice");
        System.out.println("  预置数据源：user:1 = Alice（version=" + d.source.versionOf("user:1") + "）");

        long t1 = System.nanoTime();
        Optional<String> first = d.cache.get("user:1");
        long cost1 = millisSince(t1);
        System.out.printf("  [1] 第一次 get(\"user:1\") -> %s，耗时 %d ms%n", fmt(first), cost1);
        System.out.println("      链路：L1 miss -> L2 miss -> L3 回源 -> 回填 L2 -> 回填 L1");
        System.out.printf("      回填结果：L2 version=%d，L1 version=%d%n",
                d.l2.getEntry("user:1").map(CacheEntry::version).orElse(-1L),
                d.l1.getEntry("user:1").map(CacheEntry::version).orElse(-1L));
        int readsAfterFirst = d.source.readCount();

        long t2 = System.nanoTime();
        Optional<String> second = d.cache.get("user:1");
        long cost2 = millisSince(t2);
        System.out.printf("  [2] 第二次 get(\"user:1\") -> %s，耗时 %d ms%n", fmt(second), cost2);
        System.out.println("      链路：L1 命中，未访问 L2 / L3");

        System.out.printf("  指标：L3 回源次数 %d -> %d，L1 命中 %d 次，L2 命中 %d 次，L1 未命中 %d 次%n",
                readsAfterFirst, d.source.readCount(),
                d.metrics.hitL1.get(), d.metrics.hitL2.get(), d.metrics.missL1.get());
        System.out.println("  结论：第一次请求承担回源成本，第二次请求被 L1 完全拦截（耗时下降）");

        results.add(new ScenarioResult("1", "冷读穿透回源 + L1 命中", "回填 L1/L2，二次命中 L1",
                d.source.readCount(), d.metrics.hitRate()));
    }

    // ---------------------------------------------------------------- 场景 2

    /**
     * 场景 2：多线程并发读取同一个 Key，由 SingleFlight 合并，只回源 1 次。
     */
    private void scenario2SingleFlight() throws Exception {
        printTitle("场景 2 / 6", "并发同 Key 回源合并（SingleFlight），只打穿数据源 1 次");
        Deployment d = deploy();
        d.source.put("hot:100", "HotValue");
        // 拉长回源耗时，制造并发窗口
        d.source.readDelayMillis(150L);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Optional<String>> values = new ArrayList<>();

        long t0 = System.nanoTime();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    Optional<String> value = d.cache.get("hot:100");
                    synchronized (values) {
                        values.add(value);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        done.await(10, TimeUnit.SECONDS);
        pool.shutdownNow();
        long cost = millisSince(t0);

        long nonEmpty = values.stream().filter(Optional::isPresent).count();
        int reads = d.source.readCount();
        System.out.printf("  并发线程数：%d，统一放行后同时 get(\"hot:100\")%n", threads);
        System.out.printf("  全部返回：%d / %d 个非空结果，整体耗时 %d ms（含 %d ms 模拟回源延迟）%n",
                nonEmpty, threads, cost, 150);
        System.out.printf("  指标：L3 回源次数 %d，SingleFlight 合并次数 %d%n",
                reads, d.singleFlight.mergeCount());
        System.out.println("  结论：只有第一个线程真正回源，其余线程复用了同一个结果，数据源压力被削平");

        results.add(new ScenarioResult("2", "并发同 Key 回源合并", "16 线程仅回源 " + reads + " 次",
                reads, d.metrics.hitRate()));
    }

    // ---------------------------------------------------------------- 场景 3

    /**
     * 场景 3：空值占位防穿透。
     */
    private void scenario3NullPlaceholder() {
        printTitle("场景 3 / 6", "空值占位防穿透：查不存在的 Key 只打穿数据源 1 次");
        Deployment d = deploy();
        System.out.println("  预置数据源：空（user:404 在数据源中确实不存在）");

        Optional<String> first = d.cache.get("user:404");
        System.out.printf("  [1] 第一次 get(\"user:404\") -> %s（数据源返回不存在）%n", fmt(first));
        int readsAfterFirst = d.source.readCount();
        System.out.println("      处理：写入短 TTL（1 分钟）空值占位符到 L2 与 L1");

        Optional<String> second = d.cache.get("user:404");
        Optional<String> third = d.cache.get("user:404");
        System.out.printf("  [2] 第二次 get(\"user:404\") -> %s，[3] 第三次 -> %s%n", fmt(second), fmt(third));
        System.out.println("      链路：命中 L1 空值占位，直接返回 empty，不再访问 L2 / L3");
        System.out.printf("  指标：L3 回源次数 %d -> %d，空值占位命中 %d 次%n",
                readsAfterFirst, d.source.readCount(), d.metrics.nullHit.get());
        System.out.println("  结论：空值也进缓存，热点不存在 Key 的穿透流量被挡住");

        results.add(new ScenarioResult("3", "空值占位防穿透", "回源 1 次，命中空值占位 "
                + d.metrics.nullHit.get() + " 次", d.source.readCount(), d.metrics.hitRate()));
    }

    // ---------------------------------------------------------------- 场景 4

    /**
     * 场景 4：写操作触发本地清除与失效广播（两个实例共享 L2 与失效总线）。
     */
    private void scenario4WriteInvalidation() {
        printTitle("场景 4 / 6", "写操作清除本地缓存 + 广播失效事件，其他实例同步失效");
        InMemoryL2Cache<String, String> sharedL2 = new InMemoryL2Cache<>("demo-L2(shared)");
        DemoSource sharedSource = new DemoSource();
        InMemoryInvalidationBus bus = new InMemoryInvalidationBus();

        Deployment instanceA = newDeployment(new ConcurrentHashMapL1Cache<>("instance-A-L1"), sharedL2, sharedSource, bus);
        Deployment instanceB = newDeployment(new ConcurrentHashMapL1Cache<>("instance-B-L1"), sharedL2, sharedSource, bus);
        sharedSource.put("order:1001", "v1");
        System.out.printf("  拓扑：实例 A 与实例 B 各自持有 L1，共享同一份 L2 与失效总线（订阅者 %d 个）%n",
                bus.subscriberCount());

        System.out.printf("  [1] 实例 A get(\"order:1001\") -> %s%n", fmt(instanceA.cache.get("order:1001")));
        System.out.printf("      实例 B get(\"order:1001\") -> %s（命中共享 L2 并回填 B 的 L1）%n",
                fmt(instanceB.cache.get("order:1001")));
        System.out.printf("      此时 A.L1 条目数=%d，B.L1 条目数=%d，L2 条目数=%d%n",
                instanceA.l1.size(), instanceB.l1.size(), sharedL2.size());

        int readsBeforeWrite = sharedSource.readCount();
        System.out.println("  [2] 实例 A put(\"order:1001\", \"v2\")：先写数据源，再删 L2 / 本地 L1，最后广播事件");
        instanceA.cache.put("order:1001", "v2");
        System.out.printf("      数据源写入次数=%d，事件广播 subscriber=%d，A 收到失效=%d 次，B 收到失效=%d 次%n",
                sharedSource.writeCount(), bus.subscriberCount(),
                instanceA.metrics.invalidation.get(), instanceB.metrics.invalidation.get());
        System.out.printf("      写后状态：A.L1 条目数=%d，B.L1 条目数=%d，L2 条目数=%d%n",
                instanceA.l1.size(), instanceB.l1.size(), sharedL2.size());

        Optional<String> afterWriteOnB = instanceB.cache.get("order:1001");
        System.out.printf("  [3] 实例 B 再次 get(\"order:1001\") -> %s（B 的 L1 已被广播清空，重新回源取到新值）%n",
                fmt(afterWriteOnB));
        System.out.printf("  指标：共享数据源回源次数 %d -> %d（写入后 B 侧重新回源 1 次）%n",
                readsBeforeWrite, sharedSource.readCount());
        System.out.println("  结论：写路径不更新缓存而是删缓存，靠广播 + 版本号保证两个实例都看不到旧值");

        results.add(new ScenarioResult("4", "写触发清除 + 失效广播", "B 侧同步失效并读到 v2",
                sharedSource.readCount(), instanceB.metrics.hitRate()));
    }

    // ---------------------------------------------------------------- 场景 5

    /**
     * 场景 5：旧版本失效事件被丢弃。
     */
    private void scenario5StaleEventDiscarded() {
        printTitle("场景 5 / 6", "乱序保护：晚到的旧版本失效事件被丢弃，不误删新数据");
        Deployment d = deploy();
        d.source.put("config:1", "v-new");
        long version = d.source.versionOf("config:1");
        System.out.printf("  预置数据源：config:1 = v-new（version=%d），且已读入 L1%n", version);
        d.cache.get("config:1");
        System.out.printf("      L1 当前条目：version=%d%n",
                d.l1.getEntry("config:1").map(CacheEntry::version).orElse(-1L));

        System.out.println("  [1] 手工投递一个“晚到的旧事件”（version=" + (version - 1) + "）");
        d.bus.publish(event("config:1", version - 1));
        boolean stillThere = d.l1.getEntry("config:1").isPresent();
        System.out.printf("      L1 条目是否仍在：%s；版本过旧被忽略次数=%d%n",
                stillThere, d.metrics.staleIgnored.get());

        System.out.println("  [2] 再投递一个“新事件”（version=" + version + "）");
        d.bus.publish(event("config:1", version));
        boolean evicted = d.l1.getEntry("config:1").isEmpty();
        System.out.printf("      L1 条目已被清除：%s；成功处理的失效次数=%d%n",
                evicted, d.metrics.invalidation.get());

        int readsBefore = d.source.readCount();
        Optional<String> after = d.cache.get("config:1");
        System.out.printf("  [3] 再次 get(\"config:1\") -> %s（回源次数 %d -> %d）%n",
                fmt(after), readsBefore, d.source.readCount());
        System.out.println("  结论：version 比较拦住了乱序事件，避免旧事件把新数据清掉后引出脏读");

        results.add(new ScenarioResult("5", "旧版本事件丢弃", "旧事件被忽略，新事件生效",
                d.source.readCount(), d.metrics.hitRate()));
    }

    // ---------------------------------------------------------------- 场景 6

    /**
     * 场景 6：数据源故障时降级返回旧值。
     */
    private void scenario6FailureFallbackToStale() {
        printTitle("场景 6 / 6", "数据源故障降级：用 L1 旧值兜底，业务仍拿到可用数据");
        ConcurrentHashMapL1Cache<String, String> rawL1 =
                new ConcurrentHashMapL1Cache<>("demo-L1(miss-once)");
        MissOnceL1 l1 = new MissOnceL1(rawL1);
        InMemoryL2Cache<String, String> l2 = new InMemoryL2Cache<>("demo-L2");
        DemoSource source = new DemoSource();
        InMemoryInvalidationBus bus = new InMemoryInvalidationBus();
        DemoMetrics metrics = new DemoMetrics();
        SingleFlight<String, String> singleFlight = new SingleFlight<>();
        MultiLevelCache<String, String> cache = new MultiLevelCache<>(
                l1, l2, source, source,
                new CacheAsideReadStrategy<>(singleFlight),
                new CacheAsideWriteStrategy<>(),
                bus, metrics);

        source.put("product:7", "product-v1");
        System.out.printf("  [1] 正常读 get(\"product:7\") -> %s（回填 L1 旧值，version=%d）%n",
                fmt(cache.get("product:7")),
                rawL1.getEntry("product:7").map(CacheEntry::version).orElse(-1L));

        source.failWith(new IllegalStateException("L3 数据源连接超时"));
        l2.evict("product:7");
        l1.forceMissOnce();
        System.out.println("  [2] 注入故障：数据源抛异常；同时 L2 条目被淘汰、L1 本次读判定过期");
        System.out.println("      链路：L1 未命中 -> L2 未命中 -> L3 抛异常 -> 降级策略回看旧值（peekStale）");

        Optional<String> fallback = cache.get("product:7");
        System.out.printf("  [3] 故障期间 get(\"product:7\") -> %s%n", fmt(fallback));
        System.out.printf("      数据源异常次数=%d，回源耗时被记录 %d 次%n",
                source.failureCount(), metrics.loadTimeCount.get());
        System.out.println("  结论：源不可用时返回旧值而不是抛异常，为故障恢复争取时间（旧值可从 L1/L2 兜底）");

        source.recover();
        results.add(new ScenarioResult("6", "源故障降级返回旧值",
                "故障期间仍返回 " + fmt(fallback), source.readCount(), metrics.hitRate()));
    }

    // ---------------------------------------------------------------- 汇总

    private void printSummary() {
        System.out.println();
        System.out.println(HEAVY_LINE);
        System.out.println("汇总：本次演示的 6 个场景");
        System.out.println(HEAVY_LINE);
        System.out.println(pad("序号", 6) + pad("场景", 26) + pad("结果", 30)
                + pad("L3 回源", 10) + "命中率");
        System.out.println(THIN_LINE);
        for (ScenarioResult r : results) {
            System.out.println(pad(r.index(), 6) + pad(r.name(), 26) + pad(r.outcome(), 30)
                    + pad(r.sourceReads() + " 次", 10) + r.hitRate());
        }
        System.out.println(THIN_LINE);
        System.out.println("说明：命中率 = (L1 命中 + L2 命中) / 请求数，请求数 = L1 命中 + L1 未命中；");
        System.out.println("      L2 命中指“L1 未命中但 L2 命中”，空值占位命中计入 L1 命中。");
        System.out.println("      场景 2 / 5 的请求均为首次访问（并发冷读、失效后重建），");
        System.out.println("      命中率天然为 0，属于预期结果，而非缓存失效。");
        System.out.println();
        System.out.println("全部 6 个场景执行完毕：多级读取、并发合并、穿透防护、");
        System.out.println("一致性广播、乱序保护、故障降级六条链路均已跑通。");
        System.out.println(HEAVY_LINE);
    }

    // ---------------------------------------------------------------- 装配

    /** 一次独立的纯内存部署：L1 + L2 + 数据源 + 失效总线 + 指标 + SingleFlight + 编排器。 */
    private static final class Deployment {
        final ConcurrentHashMapL1Cache<String, String> l1;
        final InMemoryL2Cache<String, String> l2;
        final DemoSource source;
        final InMemoryInvalidationBus bus;
        final DemoMetrics metrics;
        final SingleFlight<String, String> singleFlight;
        final MultiLevelCache<String, String> cache;

        Deployment(ConcurrentHashMapL1Cache<String, String> l1,
                   InMemoryL2Cache<String, String> l2,
                   DemoSource source,
                   InMemoryInvalidationBus bus,
                   DemoMetrics metrics,
                   SingleFlight<String, String> singleFlight,
                   MultiLevelCache<String, String> cache) {
            this.l1 = l1;
            this.l2 = l2;
            this.source = source;
            this.bus = bus;
            this.metrics = metrics;
            this.singleFlight = singleFlight;
            this.cache = cache;
        }
    }

    /** 全新的一套内存组件。 */
    private static Deployment deploy() {
        return newDeployment(new ConcurrentHashMapL1Cache<>("demo-L1"),
                new InMemoryL2Cache<>("demo-L2"),
                new DemoSource(),
                new InMemoryInvalidationBus());
    }

    /** 按给定的 L1 / L2 / 数据源 / 总线组装一套组件（用于多实例共享 L2 与总线的场景）。 */
    private static Deployment newDeployment(ConcurrentHashMapL1Cache<String, String> l1,
                                            InMemoryL2Cache<String, String> l2,
                                            DemoSource source,
                                            InMemoryInvalidationBus bus) {
        DemoMetrics metrics = new DemoMetrics();
        SingleFlight<String, String> singleFlight = new SingleFlight<>();
        MultiLevelCache<String, String> cache = new MultiLevelCache<>(
                l1, l2, source, source,
                new CacheAsideReadStrategy<>(singleFlight),
                new CacheAsideWriteStrategy<>(),
                bus, metrics);
        return new Deployment(l1, l2, source, bus, metrics, singleFlight, cache);
    }

    private static CacheInvalidationEvent event(String key, long version) {
        return new CacheInvalidationEvent(UUID.randomUUID().toString(), key, version,
                EventType.INVALIDATE, System.currentTimeMillis());
    }

    // ---------------------------------------------------------------- 测试替身

    /**
     * 内存数据源：同时实现 SourceReader 与 SourceWriter，支持回源计数、延迟与故障注入。
     * 生产环境把这两个方法接到自己的 DAO / RPC 上即可。
     */
    private static final class DemoSource
            implements SourceReader<String, String>, SourceWriter<String, String> {

        private final Map<String, String> data = new ConcurrentHashMap<>();
        private final Map<String, Long> versions = new ConcurrentHashMap<>();
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicInteger failures = new AtomicInteger();
        private final AtomicLong versionSeq = new AtomicLong();
        private volatile RuntimeException failure;
        private volatile long readDelayMillis;

        /** 预置数据，不计入写入次数。 */
        void put(String key, String value) {
            data.put(key, value);
            versions.put(key, versionSeq.incrementAndGet());
        }

        long versionOf(String key) {
            return versions.getOrDefault(key, 0L);
        }

        int readCount() {
            return reads.get();
        }

        int writeCount() {
            return writes.get();
        }

        int failureCount() {
            return failures.get();
        }

        void readDelayMillis(long millis) {
            this.readDelayMillis = millis;
        }

        void failWith(RuntimeException error) {
            this.failure = error;
        }

        void recover() {
            this.failure = null;
        }

        @Override
        public VersionedValue<String> read(String key) throws Exception {
            reads.incrementAndGet();
            if (readDelayMillis > 0L) {
                Thread.sleep(readDelayMillis);
            }
            RuntimeException error = failure;
            if (error != null) {
                failures.incrementAndGet();
                throw error;
            }
            String value = data.get(key);
            if (value == null) {
                return null;
            }
            return new VersionedValue<>(value, versions.getOrDefault(key, 0L));
        }

        @Override
        public VersionedValue<String> write(String key, String value) throws Exception {
            writes.incrementAndGet();
            RuntimeException error = failure;
            if (error != null) {
                failures.incrementAndGet();
                throw error;
            }
            long version = versionSeq.incrementAndGet();
            data.put(key, value);
            versions.put(key, version);
            return new VersionedValue<>(value, version);
        }
    }

    /**
     * 计数指标：项目只提供 NoOp 实现，演示需要把关键计数打印出来。
     */
    private static final class DemoMetrics implements CacheMetrics {

        final AtomicInteger hitL1 = new AtomicInteger();
        final AtomicInteger hitL2 = new AtomicInteger();
        final AtomicInteger missL1 = new AtomicInteger();
        final AtomicInteger missL2 = new AtomicInteger();
        final AtomicInteger nullHit = new AtomicInteger();
        final AtomicInteger invalidation = new AtomicInteger();
        final AtomicInteger staleIgnored = new AtomicInteger();
        final AtomicInteger loadTimeCount = new AtomicInteger();

        @Override
        public void recordHit(String level) {
            if ("L1".equals(level)) {
                hitL1.incrementAndGet();
            } else if ("L2".equals(level)) {
                hitL2.incrementAndGet();
            }
        }

        @Override
        public void recordMiss(String level) {
            if ("L1".equals(level)) {
                missL1.incrementAndGet();
            } else if ("L2".equals(level)) {
                missL2.incrementAndGet();
            }
        }

        @Override
        public void recordNullHit(String level) {
            nullHit.incrementAndGet();
        }

        @Override
        public void recordLoadTime(Duration duration) {
            loadTimeCount.incrementAndGet();
        }

        @Override
        public void recordLoadFailure(Throwable error) {
            // 当前只在策略层记录耗时，失败次数由数据源侧统计
        }

        @Override
        public void recordSingleFlightMerge() {
            // 合并次数直接从 SingleFlight#mergeCount 读取
        }

        @Override
        public void recordInvalidation() {
            invalidation.incrementAndGet();
        }

        @Override
        public void recordStaleEventIgnored() {
            staleIgnored.incrementAndGet();
        }

        /** 命中率 = (L1 命中 + L2 命中) / 请求数，请求数 = L1 命中 + L1 未命中。 */
        String hitRate() {
            int hits = hitL1.get() + hitL2.get();
            int total = hitL1.get() + missL1.get();
            if (total == 0) {
                return "n/a";
            }
            return String.format("%.1f%%", hits * 100.0 / total);
        }
    }

    /**
     * L1 装饰器：首次 getEntry 强制返回“未命中”，之后正常透传。
     *
     * <p>用途是模拟“本地缓存刚刚过期，但旧值仍可用于兜底”：
     * 读流程第一次查 L1 判为 miss，故障降级阶段的 peekStale 第二次查 L1 就能取到旧值。
     */
    private static final class MissOnceL1 implements CacheLevel<String, String> {

        private final CacheLevel<String, String> delegate;
        private final AtomicBoolean forceMiss = new AtomicBoolean(false);

        MissOnceL1(CacheLevel<String, String> delegate) {
            this.delegate = delegate;
        }

        void forceMissOnce() {
            forceMiss.set(true);
        }

        @Override
        public Optional<CacheEntry<String>> getEntry(String key) {
            if (forceMiss.compareAndSet(true, false)) {
                return Optional.empty();
            }
            return delegate.getEntry(key);
        }

        @Override
        public void putEntry(String key, CacheEntry<String> entry) {
            delegate.putEntry(key, entry);
        }

        @Override
        public void evict(String key) {
            delegate.evict(key);
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public String name() {
            return delegate.name();
        }
    }

    // ---------------------------------------------------------------- 输出工具

    private void printBanner() {
        System.out.println(HEAVY_LINE);
        System.out.println(" hyg-cache 多级缓存演示（纯内存，无需 Redis）");
        System.out.println(" L1: ConcurrentHashMapL1Cache | L2: InMemoryL2Cache | L3: 内存数据源");
        System.out.println(HEAVY_LINE);
        System.out.println();
    }

    private static void printTitle(String index, String title) {
        System.out.println(THIN_LINE);
        System.out.println("[" + index + "] " + title);
        System.out.println(THIN_LINE);
    }

    private static String fmt(Optional<String> value) {
        return value.map(v -> "\"" + v + "\"").orElse("<empty>");
    }

    private static long millisSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    /** 按“终端显示宽度”右补空格：中日韩字符占 2 列，保证汇总表列对齐。 */
    private static String pad(String text, int width) {
        StringBuilder sb = new StringBuilder(text);
        int current = displayWidth(text);
        while (current < width) {
            sb.append(' ');
            current++;
        }
        return sb.toString();
    }

    private static int displayWidth(String text) {
        int width = 0;
        for (int i = 0; i < text.length(); i++) {
            width += (text.charAt(i) > 0xFF) ? 2 : 1;
        }
        return width;
    }
}

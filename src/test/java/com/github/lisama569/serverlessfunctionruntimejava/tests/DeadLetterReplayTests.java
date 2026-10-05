package com.github.lisama569.serverlessfunctionruntimejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lisama569.serverlessfunctionruntimejava.model.ExecutionRecord;
import com.github.lisama569.serverlessfunctionruntimejava.model.ExecutionStatus;
import com.github.lisama569.serverlessfunctionruntimejava.model.InvocationRequest;
import com.github.lisama569.serverlessfunctionruntimejava.model.RejectionReason;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.ExecutionStore;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRegistry;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRuntime;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.InvocationRejectedException;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.RetryPolicy;

/** 死信重放：成功转终态、再耗尽回死信、并发去重、可区分拒绝、限流与顺序、容量淘汰。 */
class DeadLetterReplayTests {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayTests.class);

    private FunctionRegistry registry;
    private ExecutionStore store;
    private FunctionRuntime runtime;

    @BeforeEach
    void setUp() {
        registry = new FunctionRegistry();
        store = new ExecutionStore();
        runtime = new FunctionRuntime(registry, store);
        runtime.setRetryPolicy(new RetryPolicy(3, 10, 200));
    }

    @AfterEach
    void tearDown() {
        runtime.shutdown();
    }

    private static InvocationRequest req(String fn, String key) {
        return new InvocationRequest(fn, key, null, "payload", 0);
    }

    private static void logRecord(String tag, ExecutionRecord r) {
        log.info("[{}] idempotencyKey={} status={} attempts={} replayCount={} lastReplayOutcome={} result={} error={}",
                tag, r.getIdempotencyKey(), r.getStatus(), r.getAttempts(), r.getReplayCount(),
                r.getLastReplayOutcome(), r.getResult(), r.getErrorMessage());
    }

    @Test
    void replaySucceedsMovesToTerminalAndLeavesDeadLetter() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("flaky", p -> {
            if (calls.incrementAndGet() <= 3) throw new IllegalStateException("unstable");
            return "recovered";
        });

        ExecutionRecord r = runtime.invoke(req("flaky", "k-rp-1"));
        logRecord("before-replay", r);
        assertEquals(ExecutionStatus.DEAD_LETTERED, r.getStatus());
        assertTrue(runtime.deadLetters().contains(r));

        ExecutionRecord replayed = runtime.replay(r.getExecutionId());
        logRecord("after-replay", replayed);
        assertEquals(ExecutionStatus.SUCCEEDED, replayed.getStatus(), "重放成功应进入成功终态");
        assertEquals("recovered", replayed.getResult());
        assertEquals(1, replayed.getReplayCount());
        assertEquals(ExecutionStatus.SUCCEEDED, replayed.getLastReplayOutcome());
        assertFalse(runtime.deadLetters().contains(replayed), "重放成功后应从未结案死信中消失");
        assertEquals(4, calls.get(), "首次 3 次 + 重放 1 次");
    }

    @Test
    void replayReExhaustionReturnsToDeadLetterAndCountsReplays() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("always-fail", p -> { calls.incrementAndGet(); throw new IllegalStateException("boom"); });

        ExecutionRecord r = runtime.invoke(req("always-fail", "k-rp-2"));
        assertEquals(ExecutionStatus.DEAD_LETTERED, r.getStatus());

        ExecutionRecord first = runtime.replay(r.getExecutionId());
        logRecord("replay-1", first);
        assertEquals(ExecutionStatus.DEAD_LETTERED, first.getStatus(), "重放再次耗尽应回到死信");
        assertEquals(1, first.getReplayCount());
        assertEquals(ExecutionStatus.DEAD_LETTERED, first.getLastReplayOutcome());
        assertTrue(runtime.deadLetters().contains(first), "仍应留在死信中");
        assertEquals(1, runtime.deadLetters().size(), "回到死信不得产生重复条目");

        ExecutionRecord second = runtime.replay(r.getExecutionId());
        logRecord("replay-2", second);
        assertEquals(2, second.getReplayCount(), "重放次数应累计");
        assertEquals(9, calls.get(), "首次 3 次 + 两次重放各 3 次");
    }

    @Test
    void concurrentReplayExecutesBodyOnceWithSameConclusion() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        registry.register("flaky-slow", p -> {
            int n = calls.incrementAndGet();
            if (n <= 3) throw new IllegalStateException("unstable");
            replayEntered.countDown();
            try {
                releaseReplay.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return "recovered";
        });

        ExecutionRecord r = runtime.invoke(req("flaky-slow", "k-rp-3"));
        assertEquals(ExecutionStatus.DEAD_LETTERED, r.getStatus());

        var results = new ConcurrentLinkedQueue<ExecutionRecord>();
        Thread t1 = new Thread(() -> results.add(runtime.replay(r.getExecutionId())));
        t1.start();
        assertTrue(replayEntered.await(2, TimeUnit.SECONDS), "第一次重放应进入函数体");
        Thread t2 = new Thread(() -> results.add(runtime.replay(r.getExecutionId())));
        t2.start();
        Thread.sleep(100); // 给第二个重放进入等待的时间
        releaseReplay.countDown();
        t1.join(); t2.join();

        assertEquals(2, results.size());
        var it = results.iterator();
        ExecutionRecord r1 = it.next(), r2 = it.next();
        logRecord("concurrent-replay-1", r1);
        logRecord("concurrent-replay-2", r2);
        assertEquals(4, calls.get(), "并发重放下函数体只能真正多跑一次");
        assertEquals(r1.getExecutionId(), r2.getExecutionId());
        assertEquals(ExecutionStatus.SUCCEEDED, r1.getStatus());
        assertEquals(ExecutionStatus.SUCCEEDED, r2.getStatus(), "各方拿到的结论必须一致");
        assertEquals(1, r1.getReplayCount(), "只发生一次真正的重放");
        assertFalse(runtime.deadLetters().contains(r1));
    }

    @Test
    void replayOfNonDeadLetterOrTerminalIsRejectedDistinguishably() {
        // 不存在的执行标识
        InvocationRejectedException unknown = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay("no-such-execution"));
        assertEquals(RejectionReason.REPLAY_NOT_DEAD_LETTERED, unknown.getReason());
        log.info("[replay-unknown] reason={}", unknown.getReason());

        // 已成功终结的记录
        registry.register("echo", p -> "echo:" + p);
        ExecutionRecord ok = runtime.invoke(req("echo", "k-rp-4-ok"));
        assertEquals(ExecutionStatus.SUCCEEDED, ok.getStatus());
        InvocationRejectedException terminal = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay(ok.getExecutionId()));
        assertEquals(RejectionReason.REPLAY_ALREADY_TERMINAL, terminal.getReason());
        log.info("[replay-terminal] reason={}", terminal.getReason());

        // 存在但不在死信中的记录（超时终态）
        registry.register("sleeper", p -> {
            try { Thread.sleep(10_000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            return "never";
        });
        ExecutionRecord timedOut = runtime.invoke(new InvocationRequest("sleeper", "k-rp-4-to", null, null, 100));
        assertEquals(ExecutionStatus.TIMED_OUT, timedOut.getStatus());
        InvocationRejectedException notDl = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay(timedOut.getExecutionId()));
        assertEquals(RejectionReason.REPLAY_NOT_DEAD_LETTERED, notDl.getReason());
        log.info("[replay-not-dead-lettered] reason={}", notDl.getReason());
    }

    @Test
    void replayRespectsConcurrencyLimitAndKeepsRecordInDeadLetters() throws Exception {
        runtime.setConcurrencyLimit(1, 0);
        AtomicInteger calls = new AtomicInteger();
        registry.register("always-fail", p -> { calls.incrementAndGet(); throw new IllegalStateException("boom"); });
        ExecutionRecord dead = runtime.invoke(req("always-fail", "k-rp-5-dead"));
        assertEquals(ExecutionStatus.DEAD_LETTERED, dead.getStatus());

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        registry.register("slow", p -> {
            entered.countDown();
            try { release.await(5, TimeUnit.SECONDS); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            return "slow-done";
        });
        Thread t = new Thread(() -> runtime.invoke(req("slow", "k-rp-5-slow")));
        t.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        // 额度被占满：重放必须被明确拒绝，且记录留在死信中、不计重放次数
        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay(dead.getExecutionId()));
        assertEquals(RejectionReason.CONCURRENCY_LIMIT_EXCEEDED, ex.getReason());
        log.info("[replay-throttled] reason={} status={} replayCount={}",
                ex.getReason(), dead.getStatus(), dead.getReplayCount());
        assertEquals(ExecutionStatus.DEAD_LETTERED, dead.getStatus());
        assertEquals(0, dead.getReplayCount(), "被限流拒绝的重放不算一次真正的重放");
        assertTrue(runtime.deadLetters().contains(dead));
        assertEquals(3, calls.get(), "拒绝的重放不得执行函数体");

        release.countDown();
        t.join();
    }

    @Test
    void replayTakesEffectInSubmissionOrderOnSameOrderingKey() throws Exception {
        AtomicBoolean failing = new AtomicBoolean(true);
        List<String> effects = new CopyOnWriteArrayList<>();
        CountDownLatch bEntered = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        registry.register("ordered", p -> {
            if (failing.get()) throw new IllegalStateException("failing now");
            if ("B".equals(p)) {
                bEntered.countDown();
                releaseB.await(5, TimeUnit.SECONDS);
            }
            effects.add(p);
            return "done-" + p;
        });

        // A 在 failing 期间提交并耗尽重试进入死信
        ExecutionRecord a = runtime.invoke(new InvocationRequest("ordered", "k-rp-6-a", "ord", "A", 0));
        assertEquals(ExecutionStatus.DEAD_LETTERED, a.getStatus());
        failing.set(false);

        // B 先提交并持住顺序锁；随后提交 A 的重放与 C（显式给足超时，避免等待期间被判超时）
        Thread tb = new Thread(() -> runtime.invoke(new InvocationRequest("ordered", "k-rp-6-b", "ord", "B", 5_000)));
        tb.start();
        assertTrue(bEntered.await(2, TimeUnit.SECONDS));
        Thread ta = new Thread(() -> runtime.replay(a.getExecutionId()));
        ta.start();
        Thread.sleep(100);
        Thread tc = new Thread(() -> runtime.invoke(new InvocationRequest("ordered", "k-rp-6-c", "ord", "C", 5_000)));
        tc.start();
        Thread.sleep(100);
        releaseB.countDown();
        tb.join(); ta.join(); tc.join();

        logRecord("replayed-A", a);
        log.info("[ordering] effects={}", effects);
        assertEquals(List.of("B", "A", "C"), effects, "重放必须按提交顺序在同一键上生效，不得插队也不堵塞其他键");
        assertEquals(ExecutionStatus.SUCCEEDED, a.getStatus());
        assertEquals(1, a.getReplayCount());
    }

    @Test
    void replayEndingWithTimeoutIsTerminalAndFurtherReplayRejected() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("fail-then-hang", p -> {
            int n = calls.incrementAndGet();
            if (n <= 3) throw new IllegalStateException("unstable");
            // 重放时卡住：睡眠直到被超时中断
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return "never";
        });

        // 首次调用：3 次快速失败，耗尽重试进入死信
        ExecutionRecord r = runtime.invoke(new InvocationRequest("fail-then-hang", "k-rp-7", null, "payload", 200));
        assertEquals(ExecutionStatus.DEAD_LETTERED, r.getStatus());
        assertTrue(runtime.deadLetters().contains(r));

        // 重放以超时结束：按终态处理，移出未结案死信
        ExecutionRecord replayed = runtime.replay(r.getExecutionId());
        logRecord("replay-timed-out", replayed);
        assertEquals(ExecutionStatus.TIMED_OUT, replayed.getStatus(), "重放超时应进入超时终态");
        assertEquals(ExecutionStatus.TIMED_OUT, replayed.getLastReplayOutcome());
        assertEquals(1, replayed.getReplayCount());
        assertFalse(runtime.deadLetters().contains(replayed), "超时终态不得留在未结案死信里");
        assertEquals(4, calls.get(), "首次 3 次 + 重放 1 次");

        // 记录仍可查询，状态为超时终态
        ExecutionRecord persisted = store.findById(r.getExecutionId()).orElseThrow();
        assertEquals(ExecutionStatus.TIMED_OUT, persisted.getStatus());

        // 再次重放：可区分地拒绝（终态），函数体不得再跑
        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay(r.getExecutionId()));
        assertEquals(RejectionReason.REPLAY_ALREADY_TERMINAL, ex.getReason());
        log.info("[replay-after-timeout-terminal] reason={} status={} replayCount={}",
                ex.getReason(), replayed.getStatus(), replayed.getReplayCount());
        assertEquals(4, calls.get(), "终态记录的重复重放不得再执行函数体");
        assertEquals(1, replayed.getReplayCount(), "被拒绝的重放不计重放次数");
        assertFalse(runtime.deadLetters().contains(replayed), "拒绝后也不得重新出现在死信里");
    }

    @Test
    void concurrentDeadLetterWritesNeverEvictBelowCapacity() throws Exception {
        runtime.setConcurrencyLimit(64, 0);
        runtime.setDeadLetterCapacity(16);
        // 单次尝试、无退避：让并发线程几乎同时写入死信，最大化竞争窗口
        runtime.setRetryPolicy(new RetryPolicy(1, 0, 200));
        registry.register("always-fail", p -> { throw new IllegalStateException("boom"); });

        int total = 12; // 并发写入数低于容量上限
        runConcurrentFailures(total, "k-capnc");

        List<ExecutionRecord> dls = runtime.deadLetters();
        log.info("[capacity-no-overflow] size={} capacity=16 evictions={}", dls.size(), runtime.deadLetterEvictions());
        assertEquals(total, dls.size(), "未超限时不允许任何淘汰，一条都不能少");
        assertEquals(0, runtime.deadLetterEvictions(), "未超限不应产生淘汰计数");
    }

    @Test
    void concurrentDeadLetterWritesRespectCapacityStrictly() throws Exception {
        runtime.setConcurrencyLimit(64, 0);
        runtime.setDeadLetterCapacity(3);
        // 单次尝试、无退避：让并发线程几乎同时写入死信，最大化竞争窗口
        runtime.setRetryPolicy(new RetryPolicy(1, 0, 200));
        registry.register("always-fail", p -> { throw new IllegalStateException("boom"); });

        // 第一波：24 路并发写入，容量 3
        runConcurrentFailures(24, "k-capc-w1");
        List<ExecutionRecord> afterWave1 = runtime.deadLetters();
        log.info("[capacity-concurrent-w1] size={} evictions={}", afterWave1.size(), runtime.deadLetterEvictions());
        assertEquals(3, afterWave1.size(), "并发写入后死信数必须严格等于容量上限");
        assertEquals(21, runtime.deadLetterEvictions(), "只有确实超限的部分才允许被淘汰");
        assertEquals(3, afterWave1.stream().map(ExecutionRecord::getExecutionId).distinct().count(),
                "死信列表不得有重复条目");

        // 第二波：再压 24 路并发，上限仍然严格成立
        runConcurrentFailures(24, "k-capc-w2");
        List<ExecutionRecord> afterWave2 = runtime.deadLetters();
        log.info("[capacity-concurrent-w2] size={} evictions={}", afterWave2.size(), runtime.deadLetterEvictions());
        assertEquals(3, afterWave2.size(), "反复并发写入后死信数仍必须严格等于容量上限");
        assertEquals(45, runtime.deadLetterEvictions(), "累计淘汰数必须等于累计入队数减去容量");
        assertTrue(afterWave2.stream().allMatch(r -> r.getStatus() == ExecutionStatus.DEAD_LETTERED));
    }

    /** 并发提交 total 个必失败的调用（全部进入死信），并断言没有调用被拒绝或出错。 */
    private void runConcurrentFailures(int total, String keyPrefix) throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var records = new ConcurrentLinkedQueue<ExecutionRecord>();
        Thread[] threads = new Thread[total];
        for (int i = 0; i < total; i++) {
            String key = keyPrefix + "-" + i;
            threads[i] = new Thread(() -> {
                try {
                    gate.await();
                    records.add(runtime.invoke(req("always-fail", key)));
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
            threads[i].start();
        }
        gate.countDown();
        for (Thread t : threads) {
            t.join();
        }
        assertTrue(errors.isEmpty(), "并发写入死信不应有调用被拒绝或出错: " + errors);
        assertEquals(total, records.size());
        assertTrue(records.stream().allMatch(r -> r.getStatus() == ExecutionStatus.DEAD_LETTERED),
                "所有必失败调用都应进入死信");
    }

    @Test
    void deadLetterCapacityEvictsOldestObservably() {
        runtime.setDeadLetterCapacity(2);
        registry.register("always-fail", p -> { throw new IllegalStateException("boom"); });

        ExecutionRecord r1 = runtime.invoke(req("always-fail", "k-cap-1"));
        ExecutionRecord r2 = runtime.invoke(req("always-fail", "k-cap-2"));
        ExecutionRecord r3 = runtime.invoke(req("always-fail", "k-cap-3"));

        List<ExecutionRecord> dls = runtime.deadLetters();
        log.info("[capacity] deadLetters={} evictions={}",
                dls.stream().map(ExecutionRecord::getIdempotencyKey).toList(), runtime.deadLetterEvictions());
        assertEquals(2, dls.size(), "超过容量上限应淘汰最旧的死信");
        assertFalse(dls.contains(r1), "最旧的 k-cap-1 应被淘汰");
        assertTrue(dls.contains(r2));
        assertTrue(dls.contains(r3));
        assertEquals(1, runtime.deadLetterEvictions(), "淘汰次数必须可观测");

        // 查询语义不变：被淘汰的记录仍可按执行标识查询，状态保持 DEAD_LETTERED
        ExecutionRecord evicted = store.findById(r1.getExecutionId()).orElseThrow();
        assertEquals(ExecutionStatus.DEAD_LETTERED, evicted.getStatus());

        // 已被淘汰的记录不在死信中，重放应被可区分地拒绝
        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay(r1.getExecutionId()));
        assertEquals(RejectionReason.REPLAY_NOT_DEAD_LETTERED, ex.getReason());
    }

    @Test
    void deadLettersCanBeFilteredByFunctionName() {
        registry.register("fail-a", p -> { throw new IllegalStateException("a"); });
        registry.register("fail-b", p -> { throw new IllegalStateException("b"); });
        ExecutionRecord a = runtime.invoke(req("fail-a", "k-flt-a"));
        ExecutionRecord b = runtime.invoke(req("fail-b", "k-flt-b"));

        List<ExecutionRecord> onlyA = runtime.deadLetters("fail-a");
        log.info("[filter] fail-a -> {}", onlyA.stream().map(ExecutionRecord::getIdempotencyKey).toList());
        assertEquals(List.of(a), onlyA);
        assertEquals(2, runtime.deadLetters(null).size(), "不过滤时应返回全部死信");
        assertTrue(runtime.deadLetters("fail-b").contains(b));
    }
}

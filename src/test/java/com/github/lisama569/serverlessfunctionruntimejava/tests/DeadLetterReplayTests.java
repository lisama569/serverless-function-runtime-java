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
import com.github.lisama569.serverlessfunctionruntimejava.model.ReplayOutcome;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.ExecutionStore;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRegistry;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRuntime;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.InvocationRejectedException;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.RetryPolicy;

/** 死信重放与死信容量治理的行为测试。 */
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
        log.info("[{}] executionId={} idempotencyKey={} status={} attempts={} replayCount={} lastReplayOutcome={} result={} error={}",
                tag, r.getExecutionId(), r.getIdempotencyKey(), r.getStatus(), r.getAttempts(),
                r.getReplayCount(), r.getLastReplayOutcome(), r.getResult(), r.getErrorMessage());
    }

    @Test
    void replaySucceedsToTerminalStateAndLeavesDeadLetter() {
        log.info("scenario=replay-success: 前 3 次调用失败进死信，重放第 4 次调用成功");
        AtomicInteger calls = new AtomicInteger();
        registry.register("flaky", p -> {
            if (calls.incrementAndGet() <= 3) throw new IllegalStateException("boom");
            return "recovered";
        });

        ExecutionRecord dead = runtime.invoke(req("flaky", "k-rp-1"));
        logRecord("dead-lettered", dead);
        assertEquals(ExecutionStatus.DEAD_LETTERED, dead.getStatus());
        assertTrue(runtime.deadLetters().contains(dead));

        ExecutionRecord replayed = runtime.replay(dead.getExecutionId());
        logRecord("replayed", replayed);
        assertEquals(ExecutionStatus.SUCCEEDED, replayed.getStatus(), "重放成功应进入成功终态");
        assertEquals("recovered", replayed.getResult());
        assertEquals(1, replayed.getReplayCount(), "重放次数应累计为 1");
        assertEquals(ReplayOutcome.SUCCEEDED, replayed.getLastReplayOutcome());
        assertFalse(runtime.deadLetters().contains(replayed), "重放成功后应从未结案死信中消失");
        assertEquals(4, calls.get(), "首次 3 次 + 重放 1 次，无副作用重放");

        // 幂等结论保持：同键重复提交仍命中同一记录且不再次执行
        ExecutionRecord dup = runtime.invoke(req("flaky", "k-rp-1"));
        logRecord("dup-after-replay", dup);
        assertEquals(ExecutionStatus.SUCCEEDED, dup.getStatus());
        assertEquals(4, calls.get(), "重放成功后重复提交不得再执行函数体");
    }

    @Test
    void replayExhaustsAgainAndReturnsToDeadLetterWithCountAccumulated() {
        log.info("scenario=replay-exhausted-again: 函数始终失败，重放再次耗尽重试回到死信");
        AtomicInteger calls = new AtomicInteger();
        registry.register("always-fail", p -> { calls.incrementAndGet(); throw new IllegalStateException("boom"); });

        ExecutionRecord dead = runtime.invoke(req("always-fail", "k-rp-2"));
        assertEquals(ExecutionStatus.DEAD_LETTERED, dead.getStatus());

        ExecutionRecord r1 = runtime.replay(dead.getExecutionId());
        logRecord("replay-1", r1);
        assertEquals(ExecutionStatus.DEAD_LETTERED, r1.getStatus(), "重放再次耗尽应回到死信");
        assertEquals(1, r1.getReplayCount());
        assertEquals(ReplayOutcome.EXHAUSTED_AGAIN, r1.getLastReplayOutcome());
        assertTrue(runtime.deadLetters().contains(r1), "再次耗尽的记录应仍在死信中");
        assertEquals(6, calls.get(), "每轮重放应获得完整的 maxAttempts 重试预算");

        ExecutionRecord r2 = runtime.replay(dead.getExecutionId());
        logRecord("replay-2", r2);
        assertEquals(2, r2.getReplayCount(), "重放次数应累计");
        assertEquals(ReplayOutcome.EXHAUSTED_AGAIN, r2.getLastReplayOutcome());
        assertEquals(9, calls.get());
        assertTrue(runtime.deadLetters().contains(r2));
    }

    @Test
    void concurrentAndDuplicateReplayExecutesOnlyOnceWithConsistentConclusion() throws Exception {
        log.info("scenario=concurrent-replay: 并发/重复重放同一条死信，函数体只真正执行一次");
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        registry.register("flaky-block", p -> {
            int call = calls.incrementAndGet();
            if (call <= 3) throw new IllegalStateException("boom");
            // 重放轮：阻塞以便第二个重放请求并发到达
            replayEntered.countDown();
            try {
                releaseReplay.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return "recovered-once";
        });

        ExecutionRecord dead = runtime.invoke(req("flaky-block", "k-rp-3"));
        assertEquals(ExecutionStatus.DEAD_LETTERED, dead.getStatus());

        var conclusions = new ConcurrentLinkedQueue<ExecutionRecord>();
        Thread t1 = new Thread(() -> conclusions.add(runtime.replay(dead.getExecutionId())));
        t1.start();
        assertTrue(replayEntered.await(2, TimeUnit.SECONDS), "重放应已进入执行");

        // 在途重放期间并发发起第二次重放：不得再次执行函数体
        Thread t2 = new Thread(() -> conclusions.add(runtime.replay(dead.getExecutionId())));
        t2.start();
        Thread.sleep(150); // 给 t2 加入在途重放的时间
        releaseReplay.countDown();
        t1.join(); t2.join();

        assertEquals(2, conclusions.size());
        var it = conclusions.iterator();
        ExecutionRecord c1 = it.next(), c2 = it.next();
        logRecord("replay-party-1", c1);
        logRecord("replay-party-2", c2);
        assertEquals(4, calls.get(), "并发重放下函数体只能真正执行一次");
        assertEquals(ExecutionStatus.SUCCEEDED, c1.getStatus());
        assertEquals(ExecutionStatus.SUCCEEDED, c2.getStatus(), "各方拿到的结论必须一致");
        assertEquals(c1.getExecutionId(), c2.getExecutionId());
        assertEquals(1, c1.getReplayCount(), "只发生一次真实重放，重放次数为 1");
        assertEquals(c1.getReplayCount(), c2.getReplayCount());
    }

    @Test
    void replayOfNonDeadLetterOrUnknownRecordIsRejectedDistinguishably() {
        log.info("scenario=replay-rejected: 已终结/非死信/不存在的记录重放被可区分地拒绝");
        registry.register("echo", p -> "ok:" + p);
        ExecutionRecord succeeded = runtime.invoke(req("echo", "k-rp-4"));
        assertEquals(ExecutionStatus.SUCCEEDED, succeeded.getStatus());

        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay(succeeded.getExecutionId()));
        log.info("replay of succeeded rejected: reason={} message={}", ex.getReason(), ex.getMessage());
        assertEquals(RejectionReason.NOT_REPLAYABLE, ex.getReason(),
                "已成功终结的记录重放应被 NOT_REPLAYABLE 拒绝");

        InvocationRejectedException unknown = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay("no-such-execution"));
        log.info("replay of unknown rejected: reason={} message={}", unknown.getReason(), unknown.getMessage());
        assertEquals(RejectionReason.UNKNOWN_EXECUTION, unknown.getReason(),
                "不存在的执行标识应被 UNKNOWN_EXECUTION 拒绝");
        assertNotEquals(unknown.getReason(), ex.getReason(), "两种拒绝必须可区分");
    }

    private static void assertNotEquals(Object a, Object b, String msg) {
        assertFalse(a.equals(b), msg);
    }

    @Test
    void replayRespectsConcurrencyLimitAndDoesNotBlockUnrelatedCalls() throws Exception {
        log.info("scenario=replay-concurrency: 重放共用并发额度，超限明确拒绝且不堵不相关调用");
        runtime.setConcurrencyLimit(1, 0);
        AtomicInteger calls = new AtomicInteger();
        registry.register("always-fail", p -> { calls.incrementAndGet(); throw new IllegalStateException("boom"); });
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        registry.register("slow", p -> {
            occupied.countDown();
            release.await(5, TimeUnit.SECONDS);
            return "slow-done";
        });
        registry.register("echo", p -> "ok:" + p);

        ExecutionRecord dead = runtime.invoke(req("always-fail", "k-rp-5"));
        assertEquals(ExecutionStatus.DEAD_LETTERED, dead.getStatus());
        int callsBeforeReplay = calls.get();

        // 占满唯一并发额度
        var slowResult = new ExecutionRecord[1];
        Thread t = new Thread(() -> slowResult[0] = runtime.invoke(req("slow", "k-rp-5-slow")));
        t.start();
        assertTrue(occupied.await(2, TimeUnit.SECONDS));

        // 额度被占满时重放：必须明确拒绝，不得静默丢弃，记录保持死信原状
        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay(dead.getExecutionId()));
        log.info("replay over limit rejected: reason={} message={}", ex.getReason(), ex.getMessage());
        assertEquals(RejectionReason.CONCURRENCY_LIMIT_EXCEEDED, ex.getReason());
        assertEquals(ExecutionStatus.DEAD_LETTERED, dead.getStatus(), "被拒重放不得改写记录状态");
        assertEquals(0, dead.getReplayCount(), "未执行的重放不计入重放次数");
        assertTrue(runtime.deadLetters().contains(dead), "被拒重放的记录应仍在死信中");
        assertEquals(callsBeforeReplay, calls.get(), "被拒重放不得执行函数体");

        release.countDown();
        t.join();
        assertEquals(ExecutionStatus.SUCCEEDED, slowResult[0].getStatus());

        // 额度释放后重放可正常进行（函数仍失败 -> 回到死信）
        ExecutionRecord replayed = runtime.replay(dead.getExecutionId());
        logRecord("replayed-after-release", replayed);
        assertEquals(ExecutionStatus.DEAD_LETTERED, replayed.getStatus());
        assertEquals(1, replayed.getReplayCount());

        // 不相关调用不被重放堵住
        ExecutionRecord unrelated = runtime.invoke(req("echo", "k-rp-5-echo"));
        assertEquals(ExecutionStatus.SUCCEEDED, unrelated.getStatus());
    }

    @Test
    void replayRespectsOrderingKeySubmissionOrder() throws Exception {
        log.info("scenario=replay-ordering: 重放与同键新调用共用顺序锁，按提交顺序生效");
        AtomicInteger calls = new AtomicInteger();
        List<String> effects = new CopyOnWriteArrayList<>();
        CountDownLatch bEntered = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        registry.register("ordered", p -> {
            if (calls.incrementAndGet() <= 3) throw new IllegalStateException("boom"); // A 的首轮：进死信
            if ("B".equals(p)) {
                bEntered.countDown();
                try {
                    releaseB.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            effects.add(p);
            return "done-" + p;
        });

        // A 首轮失败进死信（原 payload=A，重放时沿用）
        ExecutionRecord deadA = runtime.invoke(
                new InvocationRequest("ordered", "k-rp-6-a", "ord-1", "A", 0));
        assertEquals(ExecutionStatus.DEAD_LETTERED, deadA.getStatus());

        // 同键新调用 B 先提交并持有顺序锁
        Thread tb = new Thread(() -> runtime.invoke(
                new InvocationRequest("ordered", "k-rp-6-b", "ord-1", "B", 0)));
        tb.start();
        assertTrue(bEntered.await(2, TimeUnit.SECONDS));

        // B 执行期间发起 A 的重放：必须排在 B 之后生效
        var replayResult = new ExecutionRecord[1];
        Thread tr = new Thread(() -> replayResult[0] = runtime.replay(deadA.getExecutionId()));
        tr.start();
        Thread.sleep(150);
        assertTrue(effects.isEmpty(), "B 未放行前，重放不得越过顺序锁先生效");
        releaseB.countDown();
        tb.join(); tr.join();

        logRecord("replayed-A", replayResult[0]);
        assertEquals(List.of("B", "A"), effects, "重放必须遵守同一顺序键上的提交顺序");
        assertEquals(ExecutionStatus.SUCCEEDED, replayResult[0].getStatus());
    }

    @Test
    void deadLetterCapacityEvictsOldestWithObservableTrail() {
        log.info("scenario=dead-letter-capacity: 超过容量上限按最旧先淘汰，淘汰可观测");
        runtime.setDeadLetterCapacity(2);
        registry.register("always-fail", p -> { throw new IllegalStateException("boom"); });

        ExecutionRecord d1 = runtime.invoke(req("always-fail", "k-cap-1"));
        ExecutionRecord d2 = runtime.invoke(req("always-fail", "k-cap-2"));
        ExecutionRecord d3 = runtime.invoke(req("always-fail", "k-cap-3"));
        logRecord("dead-1", d1);
        logRecord("dead-2", d2);
        logRecord("dead-3", d3);

        List<ExecutionRecord> open = runtime.deadLetters();
        assertEquals(2, open.size(), "死信数量不得超过容量上限");
        assertFalse(open.contains(d1), "最旧的死信应被先淘汰");
        assertTrue(open.contains(d2) && open.contains(d3));

        // 淘汰可观测、可解释
        assertEquals(1, runtime.totalEvictedDeadLetters());
        var evictions = runtime.deadLetterEvictions();
        assertEquals(1, evictions.size());
        log.info("eviction event: {}", evictions.get(0));
        assertEquals(d1.getExecutionId(), evictions.get(0).executionId());
        assertEquals("DEAD_LETTER_CAPACITY_EXCEEDED", evictions.get(0).reason());

        // 查询语义不乱：被淘汰的记录仍可按执行标识查询，状态保持 DEAD_LETTERED
        ExecutionRecord evicted = store.findById(d1.getExecutionId()).orElseThrow();
        assertEquals(ExecutionStatus.DEAD_LETTERED, evicted.getStatus());

        // 被淘汰的记录不再是未结案死信，重放应被可区分地拒绝
        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.replay(d1.getExecutionId()));
        assertEquals(RejectionReason.NOT_REPLAYABLE, ex.getReason());
    }

    @Test
    void deadLettersFilterByFunctionNameAndExposeReplayMetadata() {
        log.info("scenario=dead-letter-filter: 死信按函数名过滤，且可见重放次数与最近重放结果");
        registry.register("fail-a", p -> { throw new IllegalStateException("a"); });
        registry.register("fail-b", p -> { throw new IllegalStateException("b"); });

        ExecutionRecord a1 = runtime.invoke(req("fail-a", "k-flt-a1"));
        ExecutionRecord a2 = runtime.invoke(req("fail-a", "k-flt-a2"));
        ExecutionRecord b1 = runtime.invoke(req("fail-b", "k-flt-b1"));

        List<ExecutionRecord> onlyA = runtime.deadLetters("fail-a");
        assertEquals(2, onlyA.size());
        assertTrue(onlyA.contains(a1) && onlyA.contains(a2));
        assertFalse(onlyA.contains(b1));
        assertEquals(3, runtime.deadLetters().size());

        // 重放一次 a1（仍失败）后，过滤结果中应能看到重放元数据
        runtime.replay(a1.getExecutionId());
        ExecutionRecord a1Listed = runtime.deadLetters("fail-a").stream()
                .filter(r -> r.getExecutionId().equals(a1.getExecutionId())).findFirst().orElseThrow();
        logRecord("filtered-a1", a1Listed);
        assertEquals(1, a1Listed.getReplayCount());
        assertEquals(ReplayOutcome.EXHAUSTED_AGAIN, a1Listed.getLastReplayOutcome());
        assertEquals(0, a2.getReplayCount());
        assertEquals(ReplayOutcome.NONE, a2.getLastReplayOutcome());
    }
}

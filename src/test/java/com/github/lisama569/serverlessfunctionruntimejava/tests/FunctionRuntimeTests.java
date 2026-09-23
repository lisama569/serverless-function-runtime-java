package com.github.lisama569.serverlessfunctionruntimejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import com.github.lisama569.serverlessfunctionruntimejava.runtime.ExecutionStore;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRegistry;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRuntime;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.InvocationRejectedException;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.RetryPolicy;

class FunctionRuntimeTests {

    private static final Logger log = LoggerFactory.getLogger(FunctionRuntimeTests.class);

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
        log.info("[{}] idempotencyKey={} status={} attempts={} result={} error={}",
                tag, r.getIdempotencyKey(), r.getStatus(), r.getAttempts(), r.getResult(), r.getErrorMessage());
    }

    @Test
    void unknownFunctionIsRejectedWithDistinguishableReason() {
        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.invoke(req("missing-fn", "k-unknown")));
        assertEquals(RejectionReason.UNKNOWN_FUNCTION, ex.getReason());
    }

    @Test
    void invalidArgumentIsRejectedWithDistinguishableReason() {
        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.invoke(new InvocationRequest("fn", "  ", null, null, 0)));
        assertEquals(RejectionReason.INVALID_ARGUMENT, ex.getReason());
    }

    @Test
    void duplicateSubmissionHasNoDuplicateSideEffectAndSameConclusion() throws Exception {
        AtomicInteger sideEffects = new AtomicInteger();
        registry.register("counter", p -> "side-effect-" + sideEffects.incrementAndGet());

        // 并发重复提交同一幂等键
        var results = new ConcurrentLinkedQueue<ExecutionRecord>();
        Thread t1 = new Thread(() -> results.add(runtime.invoke(req("counter", "k-dup"))));
        Thread t2 = new Thread(() -> results.add(runtime.invoke(req("counter", "k-dup"))));
        t1.start(); t2.start(); t1.join(); t2.join();

        assertEquals(2, results.size());
        var it = results.iterator();
        ExecutionRecord r1 = it.next(), r2 = it.next();
        logRecord("dup-1", r1);
        logRecord("dup-2", r2);
        assertEquals(1, sideEffects.get(), "副作用只能发生一次");
        assertEquals(r1.getExecutionId(), r2.getExecutionId(), "重复提交应命中同一条执行记录");
        assertEquals(ExecutionStatus.SUCCEEDED, r1.getStatus());
        assertEquals(r1.getResult(), r2.getResult(), "重复提交结论必须一致");
    }

    @Test
    void retryExhaustionGoesToDeadLetter() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("always-fail", p -> { calls.incrementAndGet(); throw new IllegalStateException("boom"); });

        ExecutionRecord r = runtime.invoke(req("always-fail", "k-retry"));
        logRecord("retry-exhausted", r);
        assertEquals(ExecutionStatus.DEAD_LETTERED, r.getStatus());
        assertEquals(3, r.getAttempts(), "重试次数应达到上限");
        assertEquals(3, calls.get(), "函数体应被尝试 maxAttempts 次");
        assertTrue(runtime.deadLetters().contains(r), "重试耗尽应进入死信");

        // 重复提交同一幂等键：不得重新执行，结论与首次一致
        ExecutionRecord again = runtime.invoke(req("always-fail", "k-retry"));
        logRecord("retry-exhausted-dup", again);
        assertEquals(3, calls.get(), "重复提交不得产生重复副作用");
        assertEquals(ExecutionStatus.DEAD_LETTERED, again.getStatus());
        assertEquals(r.getExecutionId(), again.getExecutionId());
    }

    @Test
    void retrySucceedsBeforeExhaustion() {
        AtomicInteger calls = new AtomicInteger();
        registry.register("flaky", p -> {
            if (calls.incrementAndGet() < 3) throw new IllegalStateException("flaky");
            return "ok-after-retries";
        });
        ExecutionRecord r = runtime.invoke(req("flaky", "k-flaky"));
        logRecord("retry-success", r);
        assertEquals(ExecutionStatus.SUCCEEDED, r.getStatus());
        assertEquals(3, r.getAttempts());
        assertEquals("ok-after-retries", r.getResult());
    }

    @Test
    void concurrencyLimitExceededIsRejectedNotDropped() throws Exception {
        runtime.setConcurrencyLimit(1, 0);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        registry.register("slow", p -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
            return "slow-done";
        });

        var first = new ExecutionRecord[1];
        Thread t = new Thread(() -> first[0] = runtime.invoke(req("slow", "k-slow-1")));
        t.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        // 第二个调用超限：必须被明确拒绝而非静默丢弃
        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.invoke(req("slow", "k-slow-2")));
        assertEquals(RejectionReason.CONCURRENCY_LIMIT_EXCEEDED, ex.getReason());

        release.countDown();
        t.join();
        logRecord("slow-first", first[0]);
        assertEquals(ExecutionStatus.SUCCEEDED, first[0].getStatus());

        // 拒绝未消耗幂等键，可重新提交并成功
        ExecutionRecord retried = runtime.invoke(req("slow", "k-slow-2"));
        logRecord("slow-resubmitted", retried);
        assertEquals(ExecutionStatus.SUCCEEDED, retried.getStatus());
    }

    @Test
    void timeoutInterruptsAndLeavesNoPartialState() throws Exception {
        AtomicInteger interrupted = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        registry.register("sleeper", p -> {
            started.countDown();
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException ie) {
                interrupted.incrementAndGet();
                throw ie;
            }
            return "should-never-happen";
        });

        ExecutionRecord r = runtime.invoke(new InvocationRequest("sleeper", "k-timeout", null, null, 100));
        logRecord("timed-out", r);
        assertEquals(ExecutionStatus.TIMED_OUT, r.getStatus());
        assertNull(r.getResult(), "超时调用不得留下结果等半成品状态");
        assertTrue(started.await(1, TimeUnit.SECONDS));
        // 等待中断传播后，状态必须保持稳定，迟到的结果不得覆盖终态
        Thread.sleep(300);
        assertEquals(1, interrupted.get(), "超时必须中断执行中的函数");
        assertEquals(ExecutionStatus.TIMED_OUT, r.getStatus());
        assertNull(r.getResult());
    }

    @Test
    void sameOrderingKeyTakesEffectInSubmissionOrder() throws Exception {
        List<String> effects = new CopyOnWriteArrayList<>();
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        registry.register("appender", p -> {
            if ("A".equals(p)) {
                firstEntered.countDown();
                releaseFirst.await(5, TimeUnit.SECONDS);
            }
            effects.add(p);
            return "appended-" + p;
        });

        var recA = new ExecutionRecord[1];
        Thread t = new Thread(() -> recA[0] = runtime.invoke(
                new InvocationRequest("appender", "k-ord-a", "order-1", "A", 0)));
        t.start();
        assertTrue(firstEntered.await(2, TimeUnit.SECONDS));

        // B 提交时 A 仍在执行，B 必须排在 A 之后生效
        Thread tb = new Thread(() -> runtime.invoke(
                new InvocationRequest("appender", "k-ord-b", "order-1", "B", 0)));
        tb.start();
        Thread.sleep(100); // 给 B 进入排队的时间
        releaseFirst.countDown();
        t.join(); tb.join();

        logRecord("ordered-A", recA[0]);
        assertEquals(List.of("A", "B"), effects, "同一顺序键上的调用必须按提交顺序生效");
    }

    @Test
    void invocationsOfSameFunctionDoNotPolluteEachOther() {
        registry.register("echo", p -> "echo:" + p);
        ExecutionRecord r1 = runtime.invoke(new InvocationRequest("echo", "k-iso-1", null, "one", 0));
        ExecutionRecord r2 = runtime.invoke(new InvocationRequest("echo", "k-iso-2", null, "two", 0));
        logRecord("iso-1", r1);
        logRecord("iso-2", r2);
        assertEquals("echo:one", r1.getResult());
        assertEquals("echo:two", r2.getResult());
        assertEquals(ExecutionStatus.SUCCEEDED, r1.getStatus());
        assertEquals(ExecutionStatus.SUCCEEDED, r2.getStatus());
    }
}

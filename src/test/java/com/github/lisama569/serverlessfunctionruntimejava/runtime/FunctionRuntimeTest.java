package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FunctionRuntimeTest {

    private static final Logger log = LoggerFactory.getLogger(FunctionRuntimeTest.class);

    private FunctionRuntime runtime;

    @AfterEach
    void tearDown() {
        if (runtime != null) runtime.close();
    }

    private static void logRecord(String tag, ExecutionRecord r) {
        log.info("{} idempotencyKey={} status={} attempts={} reason={}",
                tag, r.idempotencyKey(), r.status(), r.attempts(), r.rejectionReason());
    }

    @Test
    void duplicateSubmissionProducesSingleSideEffectAndConsistentResult() throws Exception {
        runtime = new FunctionRuntime(4, 16);
        AtomicInteger sideEffects = new AtomicInteger();
        runtime.register(new FunctionDefinition("work",
                payload -> {
                    sideEffects.incrementAndGet();
                    Thread.sleep(50);
                    return "done-" + payload;
                },
                Duration.ofSeconds(5), RetryPolicy.noRetry()));

        // 并发重复提交同一幂等键
        int submitters = 8;
        ExecutorService pool = Executors.newFixedThreadPool(submitters);
        CountDownLatch gate = new CountDownLatch(1);
        List<ExecutionRecord> results = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < submitters; i++) {
            pool.submit(() -> {
                try {
                    gate.await();
                    results.add(runtime.invoke(new InvocationRequest("work", "idem-1", null, "p")));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(submitters, results.size());
        String executionId = results.get(0).executionId();
        for (ExecutionRecord r : results) {
            logRecord("duplicate", r);
            assertEquals(executionId, r.executionId(), "重复提交必须返回同一条执行记录");
            assertEquals(ExecutionStatus.SUCCEEDED, r.status());
        }
        assertEquals(1, sideEffects.get(), "相同幂等键不得产生重复副作用");

        // 完成后的再次重复提交仍返回同一结论
        ExecutionRecord again = runtime.invoke(new InvocationRequest("work", "idem-1", null, "p"));
        logRecord("duplicate-after", again);
        assertEquals(executionId, again.executionId());
        assertEquals(1, sideEffects.get());
    }

    @Test
    void retryExhaustionGoesToDeadLetter() {
        runtime = new FunctionRuntime(2, 8);
        AtomicInteger calls = new AtomicInteger();
        runtime.register(new FunctionDefinition("alwaysFail",
                payload -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("boom");
                },
                Duration.ofSeconds(5), new RetryPolicy(3, 10)));

        ExecutionRecord r = runtime.invoke(new InvocationRequest("alwaysFail", "idem-retry", null, "x"));
        logRecord("retry-exhausted", r);
        assertEquals(ExecutionStatus.DEAD_LETTERED, r.status());
        assertEquals(3, r.attempts(), "重试次数必须达到上限");
        assertEquals(3, calls.get());
        assertTrue(runtime.deadLetters().stream()
                .anyMatch(d -> d.idempotencyKey().equals("idem-retry")), "重试耗尽必须进入死信");
    }

    @Test
    void retryThenSuccessAndDuplicateAgreesOnFinalResult() {
        runtime = new FunctionRuntime(2, 8);
        AtomicInteger calls = new AtomicInteger();
        runtime.register(new FunctionDefinition("flaky",
                payload -> {
                    if (calls.incrementAndGet() < 3) throw new RuntimeException("transient");
                    return "ok";
                },
                Duration.ofSeconds(5), new RetryPolicy(5, 10)));

        ExecutionRecord r = runtime.invoke(new InvocationRequest("flaky", "idem-flaky", null, "x"));
        logRecord("retry-success", r);
        assertEquals(ExecutionStatus.SUCCEEDED, r.status());
        assertEquals(3, r.attempts());

        // 重复提交与重试后的最终结果一致
        ExecutionRecord dup = runtime.invoke(new InvocationRequest("flaky", "idem-flaky", null, "x"));
        logRecord("retry-success-duplicate", dup);
        assertEquals(r.executionId(), dup.executionId());
        assertEquals(ExecutionStatus.SUCCEEDED, dup.status());
        assertEquals(3, dup.attempts());
        assertEquals(3, calls.get(), "重复提交不得再次触发函数体");
    }

    @Test
    void concurrencyLimitRejectsWithDistinguishableReason() throws Exception {
        runtime = new FunctionRuntime(1, 0); // 无队列：超限即拒
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        runtime.register(new FunctionDefinition("blocking",
                payload -> {
                    entered.countDown();
                    release.await();
                    return "done";
                },
                Duration.ofSeconds(30), RetryPolicy.noRetry()));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> runtime.invoke(new InvocationRequest("blocking", "idem-first", null, "a")));
        assertTrue(entered.await(5, TimeUnit.SECONDS), "第一个调用应已进入执行");

        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.invoke(new InvocationRequest("blocking", "idem-second", null, "b")));
        assertEquals(RejectionReason.CONCURRENCY_LIMIT_EXCEEDED, ex.reason(),
                "超限必须被明确拒绝且原因可区分");

        ExecutionRecord rejected = runtime.findByIdempotencyKey("idem-second").orElseThrow();
        logRecord("concurrency-rejected", rejected);
        assertEquals(ExecutionStatus.REJECTED, rejected.status());
        assertEquals(RejectionReason.CONCURRENCY_LIMIT_EXCEEDED, rejected.rejectionReason());

        release.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    void queueFullRejectsInsteadOfSilentlyDropping() throws Exception {
        runtime = new FunctionRuntime(1, 1); // 1 执行 + 1 排队，第 3 个必须被拒
        CountDownLatch release = new CountDownLatch(1);
        runtime.register(new FunctionDefinition("slow",
                payload -> {
                    release.await();
                    return "done";
                },
                Duration.ofSeconds(30), RetryPolicy.noRetry()));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        pool.submit(() -> runtime.invoke(new InvocationRequest("slow", "idem-q1", null, "1")));
        // 等第一个占住执行位，再补一个占住队列位
        Thread.sleep(200);
        pool.submit(() -> runtime.invoke(new InvocationRequest("slow", "idem-q2", null, "2")));
        Thread.sleep(200);

        InvocationRejectedException ex = assertThrows(InvocationRejectedException.class,
                () -> runtime.invoke(new InvocationRequest("slow", "idem-q3", null, "3")));
        assertEquals(RejectionReason.QUEUE_FULL, ex.reason(), "队列满必须明确拒绝而非静默丢弃");

        ExecutionRecord rejected = runtime.findByIdempotencyKey("idem-q3").orElseThrow();
        logRecord("queue-full-rejected", rejected);
        assertEquals(ExecutionStatus.REJECTED, rejected.status());

        release.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    void timeoutInterruptsAndLeavesNoHalfDoneState() throws Exception {
        runtime = new FunctionRuntime(2, 8);
        AtomicInteger handlerFinished = new AtomicInteger();
        runtime.register(new FunctionDefinition("sleeper",
                payload -> {
                    try {
                        Thread.sleep(2_000);
                    } catch (InterruptedException ignored) {
                        // 模拟忽略中断、继续执行的函数体
                        Thread.sleep(300);
                    }
                    handlerFinished.incrementAndGet();
                    return "late-result";
                },
                Duration.ofMillis(100), RetryPolicy.noRetry()));

        ExecutionRecord r = runtime.invoke(new InvocationRequest("sleeper", "idem-timeout", null, "x"));
        logRecord("timed-out", r);
        assertEquals(ExecutionStatus.TIMED_OUT, r.status());
        assertEquals(1, r.attempts());

        // 等函数体“晚到”完成后，记录不得被改写成半成品或成功态
        Thread.sleep(3_000);
        assertEquals(1, handlerFinished.get(), "函数体最终会结束，但其结果不得生效");
        ExecutionRecord after = runtime.findByIdempotencyKey("idem-timeout").orElseThrow();
        logRecord("timed-out-stable", after);
        assertEquals(ExecutionStatus.TIMED_OUT, after.status());
        assertNull(after.result(), "被中断的调用不得留下结果");
    }

    @Test
    void sameOrderingKeyTakesEffectInSubmissionOrder() throws Exception {
        runtime = new FunctionRuntime(4, 16);
        List<String> effects = Collections.synchronizedList(new ArrayList<>());
        runtime.register(new FunctionDefinition("sequenced",
                payload -> {
                    if (payload.startsWith("slow")) Thread.sleep(300);
                    effects.add(payload);
                    return payload;
                },
                Duration.ofSeconds(10), RetryPolicy.noRetry()));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        // 慢调用先提交，快调用随后；无按键串行时快调用会先生效
        pool.submit(() -> runtime.invoke(new InvocationRequest("sequenced", "k-a", "key-1", "slow-1")));
        Thread.sleep(50); // 确保慢调用先进入运行时
        pool.submit(() -> runtime.invoke(new InvocationRequest("sequenced", "k-b", "key-1", "fast-2")));
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(List.of("slow-1", "fast-2"), effects, "同一键上的调用必须按提交顺序生效");
        logRecord("ordered-1", runtime.findByIdempotencyKey("k-a").orElseThrow());
        logRecord("ordered-2", runtime.findByIdempotencyKey("k-b").orElseThrow());
    }

    @Test
    void unknownFunctionAndInvalidArgumentAreDistinguishable() {
        runtime = new FunctionRuntime(2, 8);
        runtime.register(new FunctionDefinition("known",
                payload -> "ok", Duration.ofSeconds(5), RetryPolicy.noRetry()));

        InvocationRejectedException unknown = assertThrows(InvocationRejectedException.class,
                () -> runtime.invoke(new InvocationRequest("ghost", "idem-u1", null, "x")));
        assertEquals(RejectionReason.UNKNOWN_FUNCTION, unknown.reason());

        InvocationRejectedException invalid = assertThrows(InvocationRejectedException.class,
                () -> runtime.invoke(new InvocationRequest("known", "idem-u2", null, null)));
        assertEquals(RejectionReason.INVALID_ARGUMENT, invalid.reason());

        ExecutionRecord r1 = runtime.findByIdempotencyKey("idem-u1").orElseThrow();
        ExecutionRecord r2 = runtime.findByIdempotencyKey("idem-u2").orElseThrow();
        logRecord("rejected-unknown", r1);
        logRecord("rejected-invalid", r2);
        assertEquals(RejectionReason.UNKNOWN_FUNCTION, r1.rejectionReason());
        assertEquals(RejectionReason.INVALID_ARGUMENT, r2.rejectionReason());
        assertNotEquals(r1.rejectionReason(), r2.rejectionReason(), "失败原因必须可区分");
    }

    @Test
    void invocationsOfSameFunctionDoNotShareState() {
        runtime = new FunctionRuntime(4, 16);
        runtime.register(new FunctionDefinition("echo",
                payload -> "echo:" + payload, Duration.ofSeconds(5), RetryPolicy.noRetry()));

        ExecutionRecord a = runtime.invoke(new InvocationRequest("echo", "idem-a", null, "A"));
        ExecutionRecord b = runtime.invoke(new InvocationRequest("echo", "idem-b", null, "B"));
        logRecord("isolated-a", a);
        logRecord("isolated-b", b);
        assertEquals("echo:A", a.result());
        assertEquals("echo:B", b.result());
        assertNotEquals(a.executionId(), b.executionId());
    }
}

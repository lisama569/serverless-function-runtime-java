package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.lisama569.serverlessfunctionruntimejava.model.DeadLetterEviction;
import com.github.lisama569.serverlessfunctionruntimejava.model.ExecutionRecord;
import com.github.lisama569.serverlessfunctionruntimejava.model.ExecutionStatus;
import com.github.lisama569.serverlessfunctionruntimejava.model.InvocationRequest;
import com.github.lisama569.serverlessfunctionruntimejava.model.RejectionReason;
import com.github.lisama569.serverlessfunctionruntimejava.model.ReplayOutcome;

/**
 * 运行时核心。一次 invoke 的处理管线：
 * 入参校验 -> 函数查找 -> 幂等去重 -> 按键排序 -> 并发限流 -> 带超时的执行 -> 失败重试 / 死信。
 * 每个状态流转都写入 ExecutionStore 并打印日志（幂等键、状态、重试次数）。
 *
 * 死信重放（replay）复用同一条执行管线：同样的顺序锁、同样的并发额度、
 * 同样的超时与重试策略，只是入口从「新提交」换成「已有死信记录」，
 * 因此不会绕过限流，也不会打乱同一顺序键上的提交顺序。
 */
@Component
public class FunctionRuntime {

    private static final Logger log = LoggerFactory.getLogger(FunctionRuntime.class);

    /** 淘汰事件保留的最近条数（更早的只计入总数）。 */
    private static final int EVICTION_LOG_KEEP = 128;

    private final FunctionRegistry registry;
    private final ExecutionStore store;
    private volatile RetryPolicy retryPolicy = RetryPolicy.defaults();

    /** 并发上限与排队等待时长（0 表示超限立即拒绝）。 */
    private volatile Semaphore concurrencyPermits = new Semaphore(8, true);
    private volatile long queueWaitMillis = 0;

    private final ExecutorService worker = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "fn-worker");
        t.setDaemon(true);
        return t;
    });
    /** 顺序键 -> 公平信号量，保证同一键上的调用按提交顺序串行生效。 */
    private final ConcurrentMap<String, Semaphore> orderingLocks = new ConcurrentHashMap<>();
    /** 幂等键 -> 在途执行的未来，用于重复提交等待首次执行得出结论。 */
    private final ConcurrentMap<String, CompletableFuture<ExecutionRecord>> inFlight = new ConcurrentHashMap<>();
    /** 执行标识 -> 在途重放的未来，保证同一条死信同一时刻只有一次在途重放。 */
    private final ConcurrentMap<String, CompletableFuture<ExecutionRecord>> replayInFlight = new ConcurrentHashMap<>();

    /** 死信队列（未结案）：executionId -> 记录，按进入顺序排列，便于最旧先淘汰。 */
    private final Object deadLetterLock = new Object();
    private final LinkedHashMap<String, ExecutionRecord> deadLetters = new LinkedHashMap<>();
    private volatile int deadLetterCapacity = 1_000;
    /** 淘汰事件（有界）与累计淘汰数：让容量淘汰可观测、可解释。 */
    private final Deque<DeadLetterEviction> evictionLog = new ArrayDeque<>();
    private final AtomicLong totalEvicted = new AtomicLong();

    public FunctionRuntime(FunctionRegistry registry, ExecutionStore store) {
        this.registry = registry;
        this.store = store;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public void setConcurrencyLimit(int maxConcurrent, long queueWaitMillis) {
        this.concurrencyPermits = new Semaphore(maxConcurrent, true);
        this.queueWaitMillis = queueWaitMillis;
    }

    /** 配置死信容量上限；超过上限时按最旧先淘汰。 */
    public void setDeadLetterCapacity(int deadLetterCapacity) {
        if (deadLetterCapacity < 1) {
            throw new IllegalArgumentException("deadLetterCapacity must be >= 1");
        }
        this.deadLetterCapacity = deadLetterCapacity;
        synchronized (deadLetterLock) {
            evictOverflowLocked();
        }
    }

    /** 全部未结案死信（按进入顺序）。 */
    public List<ExecutionRecord> deadLetters() {
        synchronized (deadLetterLock) {
            return List.copyOf(deadLetters.values());
        }
    }

    /** 按函数名过滤的未结案死信。 */
    public List<ExecutionRecord> deadLetters(String functionName) {
        synchronized (deadLetterLock) {
            List<ExecutionRecord> matched = new ArrayList<>();
            for (ExecutionRecord r : deadLetters.values()) {
                if (r.getFunctionName().equals(functionName)) {
                    matched.add(r);
                }
            }
            return List.copyOf(matched);
        }
    }

    /** 最近的死信淘汰事件（新事件在前）。 */
    public List<DeadLetterEviction> deadLetterEvictions() {
        synchronized (deadLetterLock) {
            return List.copyOf(evictionLog);
        }
    }

    /** 累计被淘汰的死信总数。 */
    public long totalEvictedDeadLetters() {
        return totalEvicted.get();
    }

    /**
     * 提交一次调用，同步返回最终执行记录。
     *
     * @throws InvocationRejectedException 未知函数 / 非法入参 / 并发超限（原因可区分）
     */
    public ExecutionRecord invoke(InvocationRequest request) {
        validate(request);
        ServerlessFunction function = registry.lookup(request.functionName())
                .orElseThrow(() -> new InvocationRejectedException(
                        RejectionReason.UNKNOWN_FUNCTION, "unknown function: " + request.functionName()));

        ExecutionRecord record = new ExecutionRecord(UUID.randomUUID().toString(), request);
        // 幂等去重：相同幂等键只执行一次，重复提交等待并返回首次执行的结论。
        var existing = store.putIfAbsentIdempotencyKey(request.idempotencyKey(), record);
        if (existing.isPresent()) {
            ExecutionRecord first = existing.get();
            log.info("duplicate submission idempotencyKey={} status={} attempts={}",
                    first.getIdempotencyKey(), first.getStatus(), first.getAttempts());
            CompletableFuture<ExecutionRecord> pending = inFlight.get(request.idempotencyKey());
            if (pending != null) {
                pending.join();
            }
            return first;
        }
        store.save(record);
        CompletableFuture<ExecutionRecord> completion = new CompletableFuture<>();
        inFlight.put(request.idempotencyKey(), completion);
        try {
            executeWithOrdering(request, record, function, false);
        } finally {
            completion.complete(record);
            inFlight.remove(request.idempotencyKey(), completion);
        }
        return record;
    }

    /**
     * 重放一条死信：沿用原入参、顺序约束、超时与重试策略重新执行。
     * 成功则进入 SUCCEEDED 终态并移出死信；再次耗尽重试则回到死信并累计重放次数。
     * 同一条记录同一时刻只有一次在途重放，并发/重复重放共享同一次执行与结论。
     *
     * @throws InvocationRejectedException UNKNOWN_EXECUTION（记录不存在）/
     *                                     NOT_REPLAYABLE（不在死信中或已终结）/
     *                                     CONCURRENCY_LIMIT_EXCEEDED（重放同样受并发上限约束）
     */
    public ExecutionRecord replay(String executionId) {
        if (executionId == null || executionId.isBlank()) {
            throw new InvocationRejectedException(
                    RejectionReason.INVALID_ARGUMENT, "executionId must not be blank");
        }
        ExecutionRecord record = store.findById(executionId)
                .orElseThrow(() -> new InvocationRejectedException(
                        RejectionReason.UNKNOWN_EXECUTION, "unknown execution: " + executionId));

        CompletableFuture<ExecutionRecord> mine = new CompletableFuture<>();
        CompletableFuture<ExecutionRecord> existing = replayInFlight.putIfAbsent(executionId, mine);
        if (existing != null) {
            // 已有一次在途重放：不再执行函数体，等待并分享同一份结论。
            log.info("replay join in-flight executionId={} idempotencyKey={}",
                    executionId, record.getIdempotencyKey());
            return joinReplay(existing);
        }
        try {
            doReplay(record);
            mine.complete(record);
        } catch (RuntimeException e) {
            mine.completeExceptionally(e);
            throw e;
        } finally {
            replayInFlight.remove(executionId, mine);
        }
        return record;
    }

    private ExecutionRecord joinReplay(CompletableFuture<ExecutionRecord> inFlightReplay) {
        try {
            return inFlightReplay.join();
        } catch (CompletionException ce) {
            if (ce.getCause() instanceof InvocationRejectedException rejected) {
                throw rejected; // 与在途重放保持一致的拒绝结论
            }
            throw ce;
        }
    }

    private void doReplay(ExecutionRecord record) {
        ServerlessFunction function = registry.lookup(record.getFunctionName())
                .orElseThrow(() -> new InvocationRejectedException(
                        RejectionReason.UNKNOWN_FUNCTION, "unknown function: " + record.getFunctionName()));
        synchronized (record) {
            if (record.getStatus() != ExecutionStatus.DEAD_LETTERED || !isOpenDeadLetter(record)) {
                throw new InvocationRejectedException(RejectionReason.NOT_REPLAYABLE,
                        "execution " + record.getExecutionId() + " is not an open dead letter (status="
                                + record.getStatus() + ")");
            }
            // 进入重放：先从未结案死信中摘下，成功则不再回来，耗尽则由执行管线重新入列。
            removeDeadLetter(record.getExecutionId());
            record.resetAttempts(); // 本轮重放获得完整重试预算
        }
        log.info("replay start executionId={} idempotencyKey={} replayCount={}",
                record.getExecutionId(), record.getIdempotencyKey(), record.getReplayCount() + 1);

        InvocationRequest replayRequest = new InvocationRequest(
                record.getFunctionName(), record.getIdempotencyKey(), record.getOrderingKey(),
                record.getPayload(), record.getTimeoutMillis());
        boolean rejected = false;
        try {
            executeWithOrdering(replayRequest, record, function, true);
        } catch (InvocationRejectedException e) {
            rejected = true; // 并发超限：未执行函数体，记录保持死信原状
            throw e;
        } finally {
            if (rejected || record.getStatus() == ExecutionStatus.FAILED) {
                // 未真正执行（或等待中被中断）：回到死信，不累计重放次数。
                record.transitionTo(ExecutionStatus.DEAD_LETTERED);
                addDeadLetter(record);
                log.info("replay not executed, back to dead letter executionId={} status={}",
                        record.getExecutionId(), record.getStatus());
            }
        }
        ReplayOutcome outcome = switch (record.getStatus()) {
            case SUCCEEDED -> ReplayOutcome.SUCCEEDED;
            case TIMED_OUT -> ReplayOutcome.TIMED_OUT;
            default -> ReplayOutcome.EXHAUSTED_AGAIN;
        };
        record.recordReplay(outcome);
        log.info("replay done executionId={} idempotencyKey={} status={} replayCount={} outcome={}",
                record.getExecutionId(), record.getIdempotencyKey(), record.getStatus(),
                record.getReplayCount(), outcome);
    }

    private boolean isOpenDeadLetter(ExecutionRecord record) {
        synchronized (deadLetterLock) {
            return deadLetters.get(record.getExecutionId()) == record;
        }
    }

    private void removeDeadLetter(String executionId) {
        synchronized (deadLetterLock) {
            deadLetters.remove(executionId);
        }
    }

    /** 进入死信并执行容量淘汰（最旧先出），淘汰事件落日志与事件列表。 */
    private void addDeadLetter(ExecutionRecord record) {
        synchronized (deadLetterLock) {
            deadLetters.put(record.getExecutionId(), record);
            evictOverflowLocked();
        }
    }

    private void evictOverflowLocked() {
        while (deadLetters.size() > deadLetterCapacity) {
            var eldest = deadLetters.entrySet().iterator().next();
            deadLetters.remove(eldest.getKey());
            DeadLetterEviction eviction = DeadLetterEviction.capacityEvicted(eldest.getValue());
            evictionLog.addFirst(eviction);
            while (evictionLog.size() > EVICTION_LOG_KEEP) {
                evictionLog.removeLast();
            }
            totalEvicted.incrementAndGet();
            log.warn("dead letter evicted executionId={} function={} idempotencyKey={} reason={} totalEvicted={}",
                    eviction.executionId(), eviction.functionName(), eviction.idempotencyKey(),
                    eviction.reason(), totalEvicted.get());
        }
    }

    private void validate(InvocationRequest request) {
        if (request == null
                || request.functionName() == null || request.functionName().isBlank()
                || request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new InvocationRejectedException(
                    RejectionReason.INVALID_ARGUMENT, "functionName and idempotencyKey must not be blank");
        }
    }

    /**
     * 顺序锁 + 并发额度 + 执行。replay=true 时走重放语义：
     * 并发超限只抛出可区分的拒绝，不改写记录状态、不释放幂等键（记录仍是未结案死信）。
     */
    private void executeWithOrdering(InvocationRequest request, ExecutionRecord record,
                                     ServerlessFunction function, boolean replay) {
        Semaphore orderingLock = null;
        boolean orderingAcquired = false;
        if (request.orderingKey() != null && !request.orderingKey().isBlank()) {
            orderingLock = orderingLocks.computeIfAbsent(request.orderingKey(), k -> new Semaphore(1, true));
        }
        boolean permitAcquired = false;
        try {
            if (orderingLock != null) {
                orderingLock.acquire();
                orderingAcquired = true;
            }
            if (!concurrencyPermits.tryAcquire(queueWaitMillis, TimeUnit.MILLISECONDS)) {
                if (replay) {
                    log.warn("replay rejected executionId={} idempotencyKey={} reason={}",
                            record.getExecutionId(), record.getIdempotencyKey(),
                            RejectionReason.CONCURRENCY_LIMIT_EXCEEDED);
                    throw new InvocationRejectedException(RejectionReason.CONCURRENCY_LIMIT_EXCEEDED,
                            "concurrency limit exceeded, replay of execution " + record.getExecutionId());
                }
                reject(record, RejectionReason.CONCURRENCY_LIMIT_EXCEEDED,
                        "concurrency limit exceeded, idempotencyKey=" + record.getIdempotencyKey());
                return;
            }
            permitAcquired = true;
            runWithRetries(request, record, function);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            record.transitionTo(ExecutionStatus.FAILED);
            record.setErrorMessage("interrupted while waiting");
            log.warn("interrupted idempotencyKey={} status={} attempts={}",
                    record.getIdempotencyKey(), record.getStatus(), record.getAttempts());
        } finally {
            if (permitAcquired) {
                concurrencyPermits.release();
            }
            if (orderingAcquired) {
                orderingLock.release();
            }
        }
    }

    private void reject(ExecutionRecord record, RejectionReason reason, String message) {
        record.transitionTo(ExecutionStatus.REJECTED);
        record.setRejectionReason(reason);
        record.setErrorMessage(message);
        // 拒绝未产生副作用，释放幂等键，允许调用方修正后重新提交。
        store.removeIdempotencyMapping(record.getIdempotencyKey(), record);
        log.warn("rejected idempotencyKey={} status={} attempts={} reason={}",
                record.getIdempotencyKey(), record.getStatus(), record.getAttempts(), reason);
        throw new InvocationRejectedException(reason, message);
    }

    private void runWithRetries(InvocationRequest request, ExecutionRecord record,
                                ServerlessFunction function) throws InterruptedException {
        RetryPolicy policy = retryPolicy;
        long timeout = request.timeoutMillis() > 0 ? request.timeoutMillis() : policy.defaultTimeoutMillis();
        while (true) {
            record.incrementAttempts();
            record.transitionTo(ExecutionStatus.RUNNING);
            log.info("attempt start idempotencyKey={} status={} attempts={}",
                    record.getIdempotencyKey(), record.getStatus(), record.getAttempts());
            Future<String> future = worker.submit(() -> function.apply(request.payload()));
            try {
                String result = future.get(timeout, TimeUnit.MILLISECONDS);
                synchronized (record) {
                    // 超时已被标记为终态时，迟到的结果不得覆盖，避免半成品状态。
                    if (record.getStatus() == ExecutionStatus.RUNNING) {
                        record.setResult(result);
                        record.transitionTo(ExecutionStatus.SUCCEEDED);
                    }
                }
                log.info("attempt ok idempotencyKey={} status={} attempts={}",
                        record.getIdempotencyKey(), record.getStatus(), record.getAttempts());
                return;
            } catch (TimeoutException te) {
                future.cancel(true); // 中断执行中的函数
                record.transitionTo(ExecutionStatus.TIMED_OUT);
                record.setErrorMessage("timed out after " + timeout + "ms");
                log.warn("timed out idempotencyKey={} status={} attempts={}",
                        record.getIdempotencyKey(), record.getStatus(), record.getAttempts());
                return; // 超时为终态，不重试，避免中断后再次产生副作用
            } catch (ExecutionException ee) {
                record.transitionTo(ExecutionStatus.FAILED);
                record.setErrorMessage(String.valueOf(ee.getCause()));
                log.warn("attempt failed idempotencyKey={} status={} attempts={}",
                        record.getIdempotencyKey(), record.getStatus(), record.getAttempts());
                if (record.getAttempts() < policy.maxAttempts()) {
                    record.transitionTo(ExecutionStatus.RETRYING);
                    log.info("retrying idempotencyKey={} status={} attempts={}",
                            record.getIdempotencyKey(), record.getStatus(), record.getAttempts());
                    Thread.sleep(policy.backoffMillis());
                } else {
                    record.transitionTo(ExecutionStatus.DEAD_LETTERED);
                    addDeadLetter(record);
                    log.error("dead lettered idempotencyKey={} status={} attempts={}",
                            record.getIdempotencyKey(), record.getStatus(), record.getAttempts());
                    return;
                }
            }
        }
    }

    public void shutdown() {
        worker.shutdownNow();
    }
}

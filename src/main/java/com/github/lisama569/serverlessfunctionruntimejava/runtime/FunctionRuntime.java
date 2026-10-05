package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
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

import com.github.lisama569.serverlessfunctionruntimejava.model.ExecutionRecord;
import com.github.lisama569.serverlessfunctionruntimejava.model.ExecutionStatus;
import com.github.lisama569.serverlessfunctionruntimejava.model.InvocationRequest;
import com.github.lisama569.serverlessfunctionruntimejava.model.RejectionReason;

/**
 * 运行时核心。一次 invoke 的处理管线：
 * 入参校验 -> 函数查找 -> 幂等去重 -> 按键排序 -> 并发限流 -> 带超时的执行 -> 失败重试 / 死信。
 * 每个状态流转都写入 ExecutionStore 并打印日志（幂等键、状态、重试次数）。
 */
@Component
public class FunctionRuntime {

    private static final Logger log = LoggerFactory.getLogger(FunctionRuntime.class);

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
    /** 死信队列：重试耗尽且未结案的调用（有容量上限，超限时最旧先淘汰）。 */
    private final CopyOnWriteArrayList<ExecutionRecord> deadLetters = new CopyOnWriteArrayList<>();
    /** 死信写入与淘汰的互斥锁：保证「入队 + 超限淘汰」整体原子，容量上限在并发下严格成立。 */
    private final Object deadLetterLock = new Object();
    /** 死信容量上限，超过时按最旧的先淘汰。 */
    private volatile int deadLetterCapacity = 1_000;
    /** 累计被淘汰的死信数量（可观测）。 */
    private final AtomicLong deadLetterEvictions = new AtomicLong();
    /** executionId -> 在途重放的未来，保证同一条死信同一时刻只有一次在途重放。 */
    private final ConcurrentMap<String, CompletableFuture<ExecutionRecord>> replayInFlight = new ConcurrentHashMap<>();

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

    /** 配置死信容量上限（>0），超限时最旧的死信先被淘汰。 */
    public void setDeadLetterCapacity(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("dead letter capacity must be positive");
        }
        this.deadLetterCapacity = capacity;
    }

    /** 累计被淘汰的死信数量。 */
    public long deadLetterEvictions() {
        return deadLetterEvictions.get();
    }

    public List<ExecutionRecord> deadLetters() {
        return List.copyOf(deadLetters);
    }

    /** 按函数名过滤死信（null/空白表示不过滤）。 */
    public List<ExecutionRecord> deadLetters(String functionName) {
        if (functionName == null || functionName.isBlank()) {
            return deadLetters();
        }
        return deadLetters.stream()
                .filter(r -> r.getFunctionName().equals(functionName))
                .toList();
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
     * 成功则进入 SUCCEEDED 并移出死信；以超时结束同样按终态处理并移出死信；
     * 再次耗尽重试则回到死信并累计重放次数。
     * 同一条记录同一时刻只有一次在途重放，并发/重复重放等待并返回同一结论。
     *
     * @throws InvocationRejectedException REPLAY_NOT_DEAD_LETTERED（不在死信中）/
     *                                     REPLAY_ALREADY_TERMINAL（已成功终结，或重放以超时终态结束）/
     *                                     CONCURRENCY_LIMIT_EXCEEDED（额度不足，记录留在死信）
     */
    public ExecutionRecord replay(String executionId) {
        ExecutionRecord record = store.findById(executionId)
                .orElseThrow(() -> new InvocationRejectedException(
                        RejectionReason.REPLAY_NOT_DEAD_LETTERED, "no such execution: " + executionId));
        if (record.getStatus() == ExecutionStatus.SUCCEEDED) {
            throw new InvocationRejectedException(
                    RejectionReason.REPLAY_ALREADY_TERMINAL,
                    "execution already succeeded, idempotencyKey=" + record.getIdempotencyKey());
        }
        if (record.getStatus() == ExecutionStatus.TIMED_OUT
                && record.getLastReplayOutcome() == ExecutionStatus.TIMED_OUT) {
            // 重放以超时结束即按终结处理：已移出未结案死信，之后不得再被重放。
            // （从未进过死信的超时记录走下方 REPLAY_NOT_DEAD_LETTERED 分支，语义不变。）
            throw new InvocationRejectedException(
                    RejectionReason.REPLAY_ALREADY_TERMINAL,
                    "execution terminated by replay timeout, idempotencyKey=" + record.getIdempotencyKey());
        }

        // 在途重放去重：同一时刻只允许一次重放真正执行，其余等待同一结论。
        CompletableFuture<ExecutionRecord> mine = new CompletableFuture<>();
        CompletableFuture<ExecutionRecord> existing = replayInFlight.putIfAbsent(executionId, mine);
        if (existing != null) {
            log.info("duplicate replay executionId={} idempotencyKey={}", executionId, record.getIdempotencyKey());
            try {
                return existing.join();
            } catch (CompletionException ce) {
                if (ce.getCause() instanceof InvocationRejectedException rejected) {
                    throw rejected; // 与领导方拿到同一个可区分的拒绝结论
                }
                throw ce;
            }
        }
        try {
            doReplay(record);
            mine.complete(record);
            return record;
        } catch (InvocationRejectedException rejected) {
            mine.completeExceptionally(rejected);
            throw rejected;
        } finally {
            replayInFlight.remove(executionId, mine);
        }
    }

    private void doReplay(ExecutionRecord record) {
        if (!deadLetters.contains(record)) {
            throw new InvocationRejectedException(
                    RejectionReason.REPLAY_NOT_DEAD_LETTERED,
                    "execution is not in dead letters, idempotencyKey=" + record.getIdempotencyKey());
        }
        ServerlessFunction function = registry.lookup(record.getFunctionName())
                .orElseThrow(() -> new InvocationRejectedException(
                        RejectionReason.UNKNOWN_FUNCTION, "unknown function: " + record.getFunctionName()));
        // 重放不走旁路：重建原始请求，与正常调用共用同一套排序锁与并发额度。
        InvocationRequest replayRequest = new InvocationRequest(
                record.getFunctionName(), record.getIdempotencyKey(), record.getOrderingKey(),
                record.getPayload(), record.getTimeoutMillis());
        executeWithOrdering(replayRequest, record, function, true);
        record.finishReplay(record.getStatus());
        if (record.getStatus() == ExecutionStatus.SUCCEEDED) {
            deadLetters.remove(record);
            log.info("replay ok executionId={} idempotencyKey={} status={} replayCount={} (removed from dead letters)",
                    record.getExecutionId(), record.getIdempotencyKey(),
                    record.getStatus(), record.getReplayCount());
        } else if (record.getStatus() == ExecutionStatus.TIMED_OUT) {
            // 超时即终态：按终结处理，移出未结案死信，之后不得再被重放，
            // 避免「终态记录仍挂在死信里、还能被反复重放」的自相矛盾状态。
            deadLetters.remove(record);
            log.warn("replay timed out, closed as terminal executionId={} idempotencyKey={} status={} replayCount={} (removed from dead letters)",
                    record.getExecutionId(), record.getIdempotencyKey(),
                    record.getStatus(), record.getReplayCount());
        } else {
            log.warn("replay ended executionId={} idempotencyKey={} status={} replayCount={} (stays in dead letters)",
                    record.getExecutionId(), record.getIdempotencyKey(),
                    record.getStatus(), record.getReplayCount());
        }
    }

    /**
     * 进入死信队列（幂等），超容量时最旧先淘汰；淘汰计数并打日志，记录本身仍可查询。
     * 入队与淘汰在同一把锁内完成：只有确实超过上限才淘汰最旧的一条，
     * 并发写入不会把未超限的记录提前挤出去（容量上限严格成立）。
     */
    private void enterDeadLetters(ExecutionRecord record) {
        synchronized (deadLetterLock) {
            deadLetters.addIfAbsent(record);
            while (deadLetters.size() > deadLetterCapacity) {
                ExecutionRecord evicted = deadLetters.remove(0);
                deadLetterEvictions.incrementAndGet();
                log.warn("dead letter evicted (capacity={}) executionId={} idempotencyKey={} status={} evictions={}",
                        deadLetterCapacity, evicted.getExecutionId(), evicted.getIdempotencyKey(),
                        evicted.getStatus(), deadLetterEvictions.get());
            }
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

    private void executeWithOrdering(InvocationRequest request, ExecutionRecord record,
                                     ServerlessFunction function, boolean replayMode) {
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
                if (replayMode) {
                    // 重放额度不足：明确拒绝，但记录保持 DEAD_LETTERED 且留在死信中，可稍后再次重放。
                    log.warn("replay rejected, concurrency limit exceeded idempotencyKey={} replayCount={}",
                            record.getIdempotencyKey(), record.getReplayCount());
                    throw new InvocationRejectedException(RejectionReason.CONCURRENCY_LIMIT_EXCEEDED,
                            "concurrency limit exceeded during replay, idempotencyKey=" + record.getIdempotencyKey());
                }
                reject(record, RejectionReason.CONCURRENCY_LIMIT_EXCEEDED,
                        "concurrency limit exceeded, idempotencyKey=" + record.getIdempotencyKey());
                return;
            }
            permitAcquired = true;
            if (replayMode) {
                // 拿到额度后才计为一次真正的重放：被限流拒绝的重放不 inflate 重放次数。
                record.beginReplay();
                log.info("replay start executionId={} idempotencyKey={} replayCount={}",
                        record.getExecutionId(), record.getIdempotencyKey(), record.getReplayCount());
            }
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
                    enterDeadLetters(record);
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

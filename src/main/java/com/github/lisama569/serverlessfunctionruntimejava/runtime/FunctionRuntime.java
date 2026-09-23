package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 进程内 Serverless 函数运行时：注册、触发、限流、重试、幂等与按键顺序。
 *
 * <p>状态机：PENDING → RUNNING → SUCCEEDED
 *                        ↘ FAILED_RETRYABLE → RUNNING（重试）→ DEAD_LETTERED（耗尽）
 *                        ↘ TIMED_OUT（超时中断，终态）
 *           任意前置校验/限流失败 → REJECTED（终态，原因见 RejectionReason）
 */
public class FunctionRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FunctionRuntime.class);

    private final ConcurrentHashMap<String, FunctionDefinition> registry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> orderingLocks = new ConcurrentHashMap<>();
    private final ExecutionStore store = new ExecutionStore();
    private final ThreadPoolExecutor executor;
    private final int queueCapacity;

    public FunctionRuntime(int maxConcurrency, int queueCapacity) {
        if (maxConcurrency < 1) throw new IllegalArgumentException("maxConcurrency must be >= 1");
        if (queueCapacity < 0) throw new IllegalArgumentException("queueCapacity must be >= 0");
        this.queueCapacity = queueCapacity;
        BlockingQueue<Runnable> workQueue = queueCapacity > 0
                ? new ArrayBlockingQueue<>(queueCapacity)
                : new SynchronousQueue<>();
        this.executor = new ThreadPoolExecutor(
                maxConcurrency, maxConcurrency, 0L, TimeUnit.MILLISECONDS, workQueue);
    }

    public void register(FunctionDefinition definition) {
        registry.put(definition.name(), definition);
    }

    /**
     * 触发一次调用（同步阻塞至终态）。
     * 相同幂等键的重复提交直接返回已有记录，不产生重复副作用；
     * 被拒绝的调用持久化 REJECTED 记录并抛出 {@link InvocationRejectedException}。
     */
    public ExecutionRecord invoke(InvocationRequest request) {
        ExecutionRecord record = new ExecutionRecord(
                UUID.randomUUID().toString(),
                request.functionName(),
                request.idempotencyKey(),
                request.orderingKey(),
                ExecutionStatus.PENDING, 0, null, null, null, Instant.now());

        // 幂等：同一键只受理一次，重复提交返回已有记录
        ExecutionRecord existing = store.putIfAbsent(record);
        if (existing != record) {
            // 首次执行尚未结束时，等待其到达终态，保证重复提交与首次执行结论一致
            ExecutionRecord settled = existing.isTerminal() ? existing : awaitTerminal(existing.idempotencyKey());
            log.info("duplicate submission idempotencyKey={} status={} attempts={}",
                    settled.idempotencyKey(), settled.status(), settled.attempts());
            return settled;
        }

        // 前置校验：未知函数 / 非法入参
        FunctionDefinition def = registry.get(request.functionName());
        if (def == null) {
            return reject(record, RejectionReason.UNKNOWN_FUNCTION,
                    "unknown function: " + request.functionName());
        }
        if (request.payload() == null) {
            return reject(record, RejectionReason.INVALID_ARGUMENT, "payload must not be null");
        }

        // 按键顺序：同一 orderingKey 的调用按提交顺序串行生效
        Object lock = request.orderingKey() == null
                ? new Object()
                : orderingLocks.computeIfAbsent(request.orderingKey(), k -> new Object());
        synchronized (lock) {
            return executeWithRetry(record, def, request.payload());
        }
    }

    private ExecutionRecord executeWithRetry(ExecutionRecord record, FunctionDefinition def, String payload) {
        RetryPolicy policy = def.retryPolicy();
        for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
            record = record.withStatus(ExecutionStatus.RUNNING, attempt, null, null);
            store.update(record);
            log.info("attempt idempotencyKey={} status={} attempts={}",
                    record.idempotencyKey(), record.status(), record.attempts());

            Future<String> future;
            try {
                future = executor.submit(() -> def.handler().apply(payload));
            } catch (RejectedExecutionException e) {
                RejectionReason reason = queueCapacity > 0
                        ? RejectionReason.QUEUE_FULL
                        : RejectionReason.CONCURRENCY_LIMIT_EXCEEDED;
                return reject(record, reason, "executor saturated: " + reason);
            }

            try {
                String result = future.get(def.timeout().toMillis(), TimeUnit.MILLISECONDS);
                record = record.withStatus(ExecutionStatus.SUCCEEDED, attempt, result, null);
                store.update(record);
                log.info("succeeded idempotencyKey={} status={} attempts={}",
                        record.idempotencyKey(), record.status(), record.attempts());
                return record;
            } catch (TimeoutException e) {
                // 超时中断：取消任务并进入终态，之后 handler 晚到的结果不得改写记录
                future.cancel(true);
                record = record.withStatus(ExecutionStatus.TIMED_OUT, attempt, null, "timeout");
                store.update(record);
                log.info("timed out idempotencyKey={} status={} attempts={}",
                        record.idempotencyKey(), record.status(), record.attempts());
                return record;
            } catch (ExecutionException e) {
                String error = String.valueOf(e.getCause());
                if (attempt < policy.maxAttempts()) {
                    record = record.withStatus(ExecutionStatus.FAILED_RETRYABLE, attempt, null, error);
                    store.update(record);
                    log.info("retryable failure idempotencyKey={} status={} attempts={}",
                            record.idempotencyKey(), record.status(), record.attempts());
                    sleep(policy.backoffMillis());
                } else {
                    record = record.withStatus(ExecutionStatus.DEAD_LETTERED, attempt, null, error);
                    store.update(record);
                    log.info("dead lettered idempotencyKey={} status={} attempts={}",
                            record.idempotencyKey(), record.status(), record.attempts());
                    return record;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                record = record.withStatus(ExecutionStatus.DEAD_LETTERED, attempt, null, "interrupted");
                store.update(record);
                return record;
            }
        }
        return record;
    }

    /** 轮询等待某幂等键的执行到达终态（每次执行都被超时与重试上限约束，必然终止）。 */
    private ExecutionRecord awaitTerminal(String idempotencyKey) {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
        while (System.nanoTime() < deadline) {
            ExecutionRecord r = store.findByIdempotencyKey(idempotencyKey).orElseThrow();
            if (r.isTerminal()) return r;
            sleep(10);
        }
        return store.findByIdempotencyKey(idempotencyKey).orElseThrow();
    }

    private ExecutionRecord reject(ExecutionRecord record, RejectionReason reason, String message) {
        ExecutionRecord rejected = record.rejected(reason);
        store.update(rejected);
        log.info("rejected idempotencyKey={} status={} attempts={} reason={}",
                rejected.idempotencyKey(), rejected.status(), rejected.attempts(), reason);
        throw new InvocationRejectedException(reason, message);
    }

    private static void sleep(long millis) {
        if (millis <= 0) return;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Optional<ExecutionRecord> findByIdempotencyKey(String idempotencyKey) {
        return store.findByIdempotencyKey(idempotencyKey);
    }

    public List<ExecutionRecord> deadLetters() {
        return store.findAll().stream()
                .filter(r -> r.status() == ExecutionStatus.DEAD_LETTERED)
                .toList();
    }

    public ExecutionStore store() {
        return store;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}

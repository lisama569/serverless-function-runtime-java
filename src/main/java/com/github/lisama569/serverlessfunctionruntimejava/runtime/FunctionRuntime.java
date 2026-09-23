package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
    /** 死信队列：重试耗尽的调用。 */
    private final List<ExecutionRecord> deadLetters = new CopyOnWriteArrayList<>();

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

    public List<ExecutionRecord> deadLetters() {
        return List.copyOf(deadLetters);
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
            executeWithOrdering(request, record, function);
        } finally {
            completion.complete(record);
            inFlight.remove(request.idempotencyKey(), completion);
        }
        return record;
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
                                     ServerlessFunction function) {
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
                    deadLetters.add(record);
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

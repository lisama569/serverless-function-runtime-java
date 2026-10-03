package com.github.lisama569.serverlessfunctionruntimejava.model;

import java.time.Instant;

/**
 * 一次调用的持久化执行记录。状态流转全程落盘（进程内存储）。
 */
public class ExecutionRecord {

    private final String executionId;
    private final String functionName;
    private final String idempotencyKey;
    private final String orderingKey;
    private final String payload;
    private final long timeoutMillis;
    private volatile ExecutionStatus status;
    private volatile int attempts;
    private volatile int replayCount;
    private volatile ReplayOutcome lastReplayOutcome = ReplayOutcome.NONE;
    private volatile String result;
    private volatile String errorMessage;
    private volatile RejectionReason rejectionReason;
    private final Instant createdAt;
    private volatile Instant updatedAt;

    public ExecutionRecord(String executionId, InvocationRequest request) {
        this.executionId = executionId;
        this.functionName = request.functionName();
        this.idempotencyKey = request.idempotencyKey();
        this.orderingKey = request.orderingKey();
        this.payload = request.payload();
        this.timeoutMillis = request.timeoutMillis();
        this.status = ExecutionStatus.PENDING;
        this.attempts = 0;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public synchronized void transitionTo(ExecutionStatus next) {
        this.status = next;
        this.updatedAt = Instant.now();
    }

    public synchronized void incrementAttempts() {
        this.attempts++;
        this.updatedAt = Instant.now();
    }

    /** 记录一次重放的发生与结论（重放次数累计、最近一次重放结果可观测）。 */
    public synchronized void recordReplay(ReplayOutcome outcome) {
        this.replayCount++;
        this.lastReplayOutcome = outcome;
        this.updatedAt = Instant.now();
    }

    /** 重放开始前重置尝试计数，让本轮重放获得完整的重试预算。 */
    public synchronized void resetAttempts() {
        this.attempts = 0;
        this.updatedAt = Instant.now();
    }

    public String getExecutionId() { return executionId; }
    public String getFunctionName() { return functionName; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getOrderingKey() { return orderingKey; }
    public String getPayload() { return payload; }
    public long getTimeoutMillis() { return timeoutMillis; }
    public ExecutionStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public int getReplayCount() { return replayCount; }
    public ReplayOutcome getLastReplayOutcome() { return lastReplayOutcome; }
    public String getResult() { return result; }
    public String getErrorMessage() { return errorMessage; }
    public RejectionReason getRejectionReason() { return rejectionReason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public synchronized void setResult(String result) { this.result = result; this.updatedAt = Instant.now(); }
    public synchronized void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; this.updatedAt = Instant.now(); }
    public synchronized void setRejectionReason(RejectionReason reason) { this.rejectionReason = reason; this.updatedAt = Instant.now(); }
}

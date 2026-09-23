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
    private volatile ExecutionStatus status;
    private volatile int attempts;
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

    public String getExecutionId() { return executionId; }
    public String getFunctionName() { return functionName; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getOrderingKey() { return orderingKey; }
    public String getPayload() { return payload; }
    public ExecutionStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public String getResult() { return result; }
    public String getErrorMessage() { return errorMessage; }
    public RejectionReason getRejectionReason() { return rejectionReason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public synchronized void setResult(String result) { this.result = result; this.updatedAt = Instant.now(); }
    public synchronized void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; this.updatedAt = Instant.now(); }
    public synchronized void setRejectionReason(RejectionReason reason) { this.rejectionReason = reason; this.updatedAt = Instant.now(); }
}

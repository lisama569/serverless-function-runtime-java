package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.time.Instant;

/** 一次调用的持久化执行记录（不可变，状态流转产生新实例）。 */
public record ExecutionRecord(
        String executionId,
        String functionName,
        String idempotencyKey,
        String orderingKey,
        ExecutionStatus status,
        int attempts,
        String result,
        String error,
        RejectionReason rejectionReason,
        Instant updatedAt) {

    public ExecutionRecord withStatus(ExecutionStatus newStatus, int newAttempts, String newResult, String newError) {
        return new ExecutionRecord(executionId, functionName, idempotencyKey, orderingKey,
                newStatus, newAttempts, newResult, newError, rejectionReason, Instant.now());
    }

    public ExecutionRecord rejected(RejectionReason reason) {
        return new ExecutionRecord(executionId, functionName, idempotencyKey, orderingKey,
                ExecutionStatus.REJECTED, attempts, null, null, reason, Instant.now());
    }

    public boolean isTerminal() {
        return status == ExecutionStatus.SUCCEEDED
                || status == ExecutionStatus.TIMED_OUT
                || status == ExecutionStatus.DEAD_LETTERED
                || status == ExecutionStatus.REJECTED;
    }
}

package com.github.lisama569.serverlessfunctionruntimejava.model;

import java.time.Instant;

/**
 * 一条死信容量淘汰事件。死信超过上限时按最旧先淘汰，
 * 每次淘汰都落一条事件，保证淘汰行为可观测、可解释。
 * 被淘汰的执行记录本身仍保留在 ExecutionStore 中（按 executionId 可查），
 * 只是不再属于未结案的死信集合，也不可再重放。
 */
public record DeadLetterEviction(
        String executionId,
        String functionName,
        String idempotencyKey,
        String reason,
        Instant evictedAt) {

    public static DeadLetterEviction capacityEvicted(ExecutionRecord record) {
        return new DeadLetterEviction(
                record.getExecutionId(),
                record.getFunctionName(),
                record.getIdempotencyKey(),
                "DEAD_LETTER_CAPACITY_EXCEEDED",
                Instant.now());
    }
}

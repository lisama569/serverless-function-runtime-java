package com.github.lisama569.serverlessfunctionruntimejava.model;

/** 调用被拒绝的可区分原因。 */
public enum RejectionReason {
    UNKNOWN_FUNCTION,
    INVALID_ARGUMENT,
    CONCURRENCY_LIMIT_EXCEEDED,
    QUEUE_FULL,
    /** 重放目标不在死信中（不存在、在途、或已非死信状态）。 */
    REPLAY_NOT_DEAD_LETTERED,
    /** 重放目标已成功终结，不允许重放。 */
    REPLAY_ALREADY_TERMINAL
}

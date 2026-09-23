package com.github.lisama569.serverlessfunctionruntimejava.model;

/** 调用被拒绝的可区分原因。 */
public enum RejectionReason {
    UNKNOWN_FUNCTION,
    INVALID_ARGUMENT,
    CONCURRENCY_LIMIT_EXCEEDED,
    QUEUE_FULL
}

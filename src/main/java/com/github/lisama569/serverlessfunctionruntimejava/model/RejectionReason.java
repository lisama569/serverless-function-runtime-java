package com.github.lisama569.serverlessfunctionruntimejava.model;

/** 调用被拒绝的可区分原因。 */
public enum RejectionReason {
    UNKNOWN_FUNCTION,
    INVALID_ARGUMENT,
    CONCURRENCY_LIMIT_EXCEEDED,
    QUEUE_FULL,
    /** 重放目标执行记录不存在。 */
    UNKNOWN_EXECUTION,
    /** 重放目标不在死信中（已成功终结、被驳回或已被淘汰），不可重放。 */
    NOT_REPLAYABLE
}

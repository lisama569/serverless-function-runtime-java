package com.github.lisama569.serverlessfunctionruntimejava.runtime;

/** 执行状态机中的状态。 */
public enum ExecutionStatus {
    /** 已受理，等待调度 */
    PENDING,
    /** 正在执行 */
    RUNNING,
    /** 执行成功（终态） */
    SUCCEEDED,
    /** 本次尝试失败，等待重试 */
    FAILED_RETRYABLE,
    /** 被超时中断（终态，不留半成品状态） */
    TIMED_OUT,
    /** 重试耗尽，进入死信（终态） */
    DEAD_LETTERED,
    /** 调用被拒绝（终态，原因见 RejectionReason） */
    REJECTED
}

package com.github.lisama569.serverlessfunctionruntimejava.model;

/**
 * 执行状态机：
 * PENDING -> RUNNING -> SUCCEEDED
 *                    -> FAILED -> RETRYING -> RUNNING (重试)
 *                    -> FAILED -> DEAD_LETTERED (重试耗尽)
 *                    -> TIMED_OUT (超时中断，终态)
 * 提交阶段即被拒绝的调用进入 REJECTED（终态，不进入执行）。
 */
public enum ExecutionStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    RETRYING,
    TIMED_OUT,
    DEAD_LETTERED,
    REJECTED
}

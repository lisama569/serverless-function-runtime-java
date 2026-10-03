package com.github.lisama569.serverlessfunctionruntimejava.model;

/**
 * 一次死信重放的结论（记录在 ExecutionRecord 上，便于运维观察）。
 */
public enum ReplayOutcome {
    /** 从未重放过。 */
    NONE,
    /** 重放成功，记录进入 SUCCEEDED 终态并移出死信。 */
    SUCCEEDED,
    /** 重放再次耗尽重试，记录回到死信。 */
    EXHAUSTED_AGAIN,
    /** 重放过程中超时中断。 */
    TIMED_OUT,
    /** 重放因并发超限被拒绝（未执行函数体）。 */
    REJECTED
}

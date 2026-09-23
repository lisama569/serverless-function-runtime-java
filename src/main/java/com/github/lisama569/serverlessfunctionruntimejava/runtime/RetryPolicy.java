package com.github.lisama569.serverlessfunctionruntimejava.runtime;

/**
 * 可配置重试策略。
 *
 * @param maxAttempts       最大尝试次数（含首次），达到上限仍失败则进入死信
 * @param backoffMillis     重试间隔基数
 * @param defaultTimeoutMillis 单次尝试默认超时
 */
public record RetryPolicy(int maxAttempts, long backoffMillis, long defaultTimeoutMillis) {

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, 50, 5_000);
    }
}

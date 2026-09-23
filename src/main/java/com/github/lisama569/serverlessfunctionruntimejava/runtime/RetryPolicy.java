package com.github.lisama569.serverlessfunctionruntimejava.runtime;

/** 可配置的重试策略。 */
public record RetryPolicy(int maxAttempts, long backoffMillis) {
    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (backoffMillis < 0) throw new IllegalArgumentException("backoffMillis must be >= 0");
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, 0);
    }
}

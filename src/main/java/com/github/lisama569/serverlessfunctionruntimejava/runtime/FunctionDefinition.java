package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.time.Duration;

/** 已注册函数的定义。 */
public record FunctionDefinition(
        String name,
        FunctionHandler handler,
        Duration timeout,
        RetryPolicy retryPolicy) {
    public FunctionDefinition {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name required");
        if (handler == null) throw new IllegalArgumentException("handler required");
        if (timeout == null || timeout.isNegative() || timeout.isZero())
            throw new IllegalArgumentException("timeout must be positive");
        if (retryPolicy == null) retryPolicy = RetryPolicy.noRetry();
    }
}

package com.github.lisama569.serverlessfunctionruntimejava.runtime;

/** 一次函数调用请求。orderingKey 相同的调用按提交顺序串行生效。 */
public record InvocationRequest(String functionName, String idempotencyKey, String orderingKey, String payload) {
    public InvocationRequest {
        if (functionName == null || functionName.isBlank())
            throw new IllegalArgumentException("functionName required");
        if (idempotencyKey == null || idempotencyKey.isBlank())
            throw new IllegalArgumentException("idempotencyKey required");
    }
}

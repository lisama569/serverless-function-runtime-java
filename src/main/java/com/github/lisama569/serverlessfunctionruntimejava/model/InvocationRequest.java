package com.github.lisama569.serverlessfunctionruntimejava.model;

/**
 * 一次函数调用请求。
 *
 * @param functionName   目标函数名
 * @param idempotencyKey 幂等键，相同键的重复提交只产生一次副作用
 * @param orderingKey    顺序键，同一键上的调用按提交顺序串行生效
 * @param payload        入参（JSON 字符串，可空）
 * @param timeoutMillis  单次尝试的超时时间，<=0 表示使用默认
 */
public record InvocationRequest(
        String functionName,
        String idempotencyKey,
        String orderingKey,
        String payload,
        long timeoutMillis) {
}

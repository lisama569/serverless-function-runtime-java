package com.github.lisama569.serverlessfunctionruntimejava.runtime;

/** 函数体：入参为 JSON 字符串，返回 JSON 字符串结果。 */
@FunctionalInterface
public interface FunctionHandler {
    String apply(String payload) throws Exception;
}

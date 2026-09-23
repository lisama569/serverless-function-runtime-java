package com.github.lisama569.serverlessfunctionruntimejava.runtime;

/** 函数体：入参为 payload 字符串，返回结果为字符串。 */
@FunctionalInterface
public interface ServerlessFunction {
    String apply(String payload) throws Exception;
}

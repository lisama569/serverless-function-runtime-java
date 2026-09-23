package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/** 函数注册表：按名注册与查找。 */
@Component
public class FunctionRegistry {

    private final Map<String, ServerlessFunction> functions = new ConcurrentHashMap<>();

    public void register(String name, ServerlessFunction function) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("function name must not be blank");
        }
        if (function == null) {
            throw new IllegalArgumentException("function must not be null");
        }
        functions.put(name, function);
    }

    public Optional<ServerlessFunction> lookup(String name) {
        return Optional.ofNullable(functions.get(name));
    }
}

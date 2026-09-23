package com.github.lisama569.serverlessfunctionruntimejava;

import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionDefinition;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRuntime;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.RetryPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/** 运行时装配：限流参数与演示函数。 */
@Configuration
public class RuntimeConfig {

    @Bean(destroyMethod = "close")
    FunctionRuntime functionRuntime() {
        FunctionRuntime runtime = new FunctionRuntime(4, 16);
        runtime.register(new FunctionDefinition(
                "echo",
                payload -> payload,
                Duration.ofSeconds(5),
                new RetryPolicy(3, 50)));
        runtime.register(new FunctionDefinition(
                "uppercase",
                payload -> payload.toUpperCase(),
                Duration.ofSeconds(5),
                RetryPolicy.noRetry()));
        return runtime;
    }
}

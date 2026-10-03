package com.github.lisama569.serverlessfunctionruntimejava;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRegistry;

/**
 * 内置示例函数：服务启动后即可走通
 * 「按名触发 -> 失败进入死信 -> 重放成功」整条链路，无需额外准备。
 */
@Component
public class SampleFunctions {

    private static final Logger log = LoggerFactory.getLogger(SampleFunctions.class);

    private final FunctionRegistry registry;

    public SampleFunctions(FunctionRegistry registry) {
        this.registry = registry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void registerSamples() {
        // echo：原样返回入参，用于验证基本触发。
        registry.register("echo", payload -> "echo:" + payload);

        // demo-flaky：前 3 次尝试必失败（默认重试策略 maxAttempts=3，首次触发恰好耗尽进死信），
        // 第 4 次起成功——即重放时自然恢复，可直接演示死信重放。
        AtomicInteger calls = new AtomicInteger();
        registry.register("demo-flaky", payload -> {
            int n = calls.incrementAndGet();
            if (n <= 3) {
                throw new IllegalStateException("demo-flaky still unstable, call=" + n);
            }
            return "recovered-after-" + n + "-calls:" + payload;
        });

        log.info("sample functions registered: echo, demo-flaky");
    }
}

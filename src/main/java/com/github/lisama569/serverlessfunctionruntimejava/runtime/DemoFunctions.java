package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 内置示例函数：服务启动后即可走通
 * 「按名触发 -> 失败进入死信 -> 重放成功」整条链路，无需额外准备。
 *
 * demo-flaky：默认重试策略 maxAttempts=3，该函数前 3 次调用必失败
 * （首次触发正好耗尽重试进入死信），第 4 次起成功（重放即成功）。
 */
@Component
public class DemoFunctions {

    private static final Logger log = LoggerFactory.getLogger(DemoFunctions.class);

    private final FunctionRegistry registry;
    private final AtomicInteger flakyCalls = new AtomicInteger();

    public DemoFunctions(FunctionRegistry registry) {
        this.registry = registry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void registerDemoFunctions() {
        registry.register("demo-flaky", payload -> {
            int call = flakyCalls.incrementAndGet();
            if (call <= 3) {
                throw new IllegalStateException("demo-flaky planned failure #" + call);
            }
            return "demo-flaky recovered on call #" + call + ", payload=" + payload;
        });
        registry.register("echo", payload -> "echo:" + payload);
        log.info("demo functions registered: demo-flaky, echo");
    }
}

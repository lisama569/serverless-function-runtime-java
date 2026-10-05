package com.github.lisama569.serverlessfunctionruntimejava.web;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.lisama569.serverlessfunctionruntimejava.model.ExecutionRecord;
import com.github.lisama569.serverlessfunctionruntimejava.model.InvocationRequest;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.ExecutionStore;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRuntime;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.InvocationRejectedException;

/** REST 入口：按名触发函数、查询执行记录与死信。 */
@RestController
public class FunctionController {

    private final FunctionRuntime runtime;
    private final ExecutionStore store;

    public FunctionController(FunctionRuntime runtime, ExecutionStore store) {
        this.runtime = runtime;
        this.store = store;
    }

    /**
     * 触发函数。
     * POST /functions/{name}/invoke
     * Header: X-Idempotency-Key（必填）、X-Ordering-Key（可选）、X-Timeout-Millis（可选）
     * Body: payload 字符串
     */
    @PostMapping("/functions/{name}/invoke")
    public ExecutionRecord invoke(@PathVariable String name,
                                  @RequestHeader("X-Idempotency-Key") String idempotencyKey,
                                  @RequestHeader(value = "X-Ordering-Key", required = false) String orderingKey,
                                  @RequestHeader(value = "X-Timeout-Millis", defaultValue = "0") long timeoutMillis,
                                  @RequestBody(required = false) String payload) {
        return runtime.invoke(new InvocationRequest(name, idempotencyKey, orderingKey, payload, timeoutMillis));
    }

    @GetMapping("/executions/{id}")
    public ResponseEntity<ExecutionRecord> execution(@PathVariable String id) {
        return store.findById(id).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/executions")
    public ResponseEntity<ExecutionRecord> byIdempotencyKey(@RequestParam String idempotencyKey) {
        return store.findByIdempotencyKey(idempotencyKey).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/dead-letters")
    public List<ExecutionRecord> deadLetters(@RequestParam(required = false) String functionName) {
        return runtime.deadLetters(functionName);
    }

    /**
     * 重放一条死信。
     * POST /dead-letters/{executionId}/replay
     * 成功返回重放后的执行记录；目标不在死信中返回 404，已终结（成功或重放超时结案）返回 409。
     */
    @PostMapping("/dead-letters/{executionId}/replay")
    public ExecutionRecord replay(@PathVariable String executionId) {
        return runtime.replay(executionId);
    }

    /** 拒绝原因以结构化形式返回，HTTP 状态码可区分。 */
    @ExceptionHandler(InvocationRejectedException.class)
    public ResponseEntity<Map<String, Object>> rejected(InvocationRejectedException ex) {
        HttpStatus status = switch (ex.getReason()) {
            case UNKNOWN_FUNCTION -> HttpStatus.NOT_FOUND;
            case INVALID_ARGUMENT -> HttpStatus.BAD_REQUEST;
            case CONCURRENCY_LIMIT_EXCEEDED, QUEUE_FULL -> HttpStatus.TOO_MANY_REQUESTS;
            case REPLAY_NOT_DEAD_LETTERED -> HttpStatus.NOT_FOUND;
            case REPLAY_ALREADY_TERMINAL -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(Map.of(
                "reason", ex.getReason().name(),
                "message", String.valueOf(ex.getMessage())));
    }
}

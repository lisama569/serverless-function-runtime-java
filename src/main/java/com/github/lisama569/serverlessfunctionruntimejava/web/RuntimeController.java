package com.github.lisama569.serverlessfunctionruntimejava.web;

import com.github.lisama569.serverlessfunctionruntimejava.runtime.ExecutionRecord;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.FunctionRuntime;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.InvocationRejectedException;
import com.github.lisama569.serverlessfunctionruntimejava.runtime.InvocationRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/** 运行时 HTTP 入口：触发调用、查询执行记录与死信。 */
@RestController
public class RuntimeController {

    private final FunctionRuntime runtime;

    public RuntimeController(FunctionRuntime runtime) {
        this.runtime = runtime;
    }

    public record InvokeBody(String idempotencyKey, String orderingKey, String payload) {}

    @PostMapping("/functions/{name}/invoke")
    public ExecutionRecord invoke(@PathVariable String name, @RequestBody InvokeBody body) {
        return runtime.invoke(new InvocationRequest(name, body.idempotencyKey(), body.orderingKey(), body.payload()));
    }

    @GetMapping("/executions/{idempotencyKey}")
    public ResponseEntity<ExecutionRecord> find(@PathVariable String idempotencyKey) {
        return runtime.findByIdempotencyKey(idempotencyKey)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/dead-letters")
    public List<ExecutionRecord> deadLetters() {
        return runtime.deadLetters();
    }

    @ExceptionHandler(InvocationRejectedException.class)
    public ResponseEntity<Map<String, String>> rejected(InvocationRejectedException e) {
        HttpStatus status = switch (e.reason()) {
            case UNKNOWN_FUNCTION -> HttpStatus.NOT_FOUND;
            case INVALID_ARGUMENT -> HttpStatus.BAD_REQUEST;
            case CONCURRENCY_LIMIT_EXCEEDED, QUEUE_FULL -> HttpStatus.TOO_MANY_REQUESTS;
        };
        return ResponseEntity.status(status)
                .body(Map.of("reason", e.reason().name(), "message", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("reason", "INVALID_ARGUMENT", "message", e.getMessage()));
    }
}

# Serverless Function Runtime（进程内模拟）

一个本地可复现的 Serverless 函数调用与持久化执行运行时：以进程内存储与调度模拟函数注册、
触发与状态流转，保证并发、重试与重复提交下的执行结论一致且可解释。

## 调用约定

- **注册**：`FunctionRuntime.register(FunctionDefinition)`，定义包含函数名、处理器
  `FunctionHandler`（`String payload -> String result`）、超时时间与重试策略。
- **触发**：`runtime.invoke(InvocationRequest)`，请求字段：
  - `functionName`：目标函数名（必填）；
  - `idempotencyKey`：幂等键（必填），相同键的重复提交只执行一次；
  - `orderingKey`：可选，相同键的调用按提交顺序串行生效；
  - `payload`：JSON 字符串入参（必填，非 null）。
- **返回**：`invoke` 同步阻塞到终态，返回不可变的 `ExecutionRecord`
  （执行 ID、状态、重试次数、结果/错误、拒绝原因、更新时间）。
- **HTTP 入口**（Spring Boot）：
  - `POST /functions/{name}/invoke`，body：`{"idempotencyKey": "...", "orderingKey": "...", "payload": "..."}`
  - `GET /executions/{idempotencyKey}`：查询执行记录；
  - `GET /dead-letters`：列出死信。
  - 被拒绝的调用按原因映射：`UNKNOWN_FUNCTION→404`、`INVALID_ARGUMENT→400`、
    `CONCURRENCY_LIMIT_EXCEEDED / QUEUE_FULL→429`，响应体含 `reason` 字段。

## 执行状态机

```
                 校验/限流失败
PENDING ───────────────────────────► REJECTED（终态，reason 可区分）
   │
   ▼
RUNNING ── 成功 ──► SUCCEEDED（终态）
   │
   ├── 失败且可重试 ──► FAILED_RETRYABLE ──（退避后）──► RUNNING
   │
   ├── 重试耗尽 ──────► DEAD_LETTERED（终态，进入死信）
   │
   └── 超时中断 ──────► TIMED_OUT（终态）
```

- 所有状态流转都会持久化到 `ExecutionStore`（按执行 ID 与幂等键双索引），
  每次流转产生新的不可变记录。
- `TIMED_OUT` 后函数体即使“晚到”完成，其结果也不会改写记录，不留半成品状态。

## 重试与幂等规则

- **重试策略**：`RetryPolicy(maxAttempts, backoffMillis)` 按函数配置；
  失败进入 `FAILED_RETRYABLE` 并按固定退避重试，达到 `maxAttempts` 后进入
  `DEAD_LETTERED` 并出现在死信列表。`RetryPolicy.noRetry()` 表示不重试。
- **幂等**：受理时以 `idempotencyKey` 原子占位（`putIfAbsent`）。相同键的重复提交
  不会再次触发函数体；若首次执行仍在进行，重复提交方会等待其到达终态，
  返回与首次执行完全一致的最终记录（同一 `executionId`、同一状态与重试次数）。
- 因此“重试后的成功”与“重复提交”观察到的结论必然一致。

## 并发与背压策略

- 运行时以固定线程池（`maxConcurrency`）+ 有界队列（`queueCapacity`）调度：
  - 执行位与队列均满时，调用被**明确拒绝**（`QUEUE_FULL`），绝不静默丢弃；
  - `queueCapacity = 0` 时退化为纯限流，超限即拒（`CONCURRENCY_LIMIT_EXCEEDED`）。
- 每次调用受函数级超时约束：超时即中断（`Future.cancel(true)`）并置 `TIMED_OUT`。
- **按键顺序**：`orderingKey` 相同的调用在 per-key 锁下串行执行，按提交顺序生效；
  不同键之间互不阻塞。
- 同一函数的多次调用各自持有独立的 `ExecutionRecord` 与 payload，互不污染状态。

## 失败原因的可区分性

| 场景 | 状态 | 原因 |
| --- | --- | --- |
| 未知函数 | `REJECTED` | `UNKNOWN_FUNCTION` |
| 非法入参（payload 为 null 等） | `REJECTED` | `INVALID_ARGUMENT` |
| 超出并发额度（无队列） | `REJECTED` | `CONCURRENCY_LIMIT_EXCEEDED` |
| 队列满 | `REJECTED` | `QUEUE_FULL` |
| 超时中断 | `TIMED_OUT` | — |
| 重试耗尽 | `DEAD_LETTERED` | — |

## 本地验证

```bash
# 运行全部测试（覆盖重复提交、重试耗尽、并发超限、队列满、超时中断、按键顺序）
mvn test

# 启动本地服务并手动触发
mvn spring-boot:run
curl -X POST localhost:8080/functions/echo/invoke \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"demo-1","payload":"hello"}'
curl localhost:8080/executions/demo-1
curl localhost:8080/dead-letters
```

单测与运行时日志均会打印 `idempotencyKey`、`status` 与 `attempts`（重试次数），
例如：

```
INFO FunctionRuntime -- retryable failure idempotencyKey=idem-flaky status=FAILED_RETRYABLE attempts=1
INFO FunctionRuntime -- duplicate submission idempotencyKey=idem-1 status=SUCCEEDED attempts=1
```

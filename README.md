# Serverless Function Runtime（本地可复现）

进程内的 Serverless 函数调用与持久化执行运行时：以内存存储与调度模拟函数注册、
按名触发、执行状态流转、失败重试、死信、幂等去重、并发限流、超时中断中断与按键顺序保证。

## 模块结构

```
runtime/FunctionRegistry      函数注册表（按名注册/查找）
runtime/FunctionRuntime       运行时核心：校验→幂等→排序→限流→超时执行→重试/死信
runtime/ExecutionStore        进程内执行记录存储（executionId 主索引 + idempotencyKey 唯一索引）
runtime/RetryPolicy           可配置重试策略（maxAttempts / backoffMillis / defaultTimeoutMillis）
model/ExecutionRecord         持久化执行记录（状态、重试次数、结果、拒绝原因）
model/ExecutionStatus         执行状态机
model/RejectionReason         可区分的拒绝原因
web/FunctionController        REST 入口
```

## 调用约定

### 编程接口

```java
runtime.invoke(new InvocationRequest(
        functionName,    // 目标函数名，必填
        idempotencyKey,  // 幂等键，必填；相同键的重复提交只执行一次
        orderingKey,     // 顺序键，可空；相同键的调用按提交顺序串行生效
        payload,         // 入参字符串，可空
        timeoutMillis)); // 单次尝试超时，<=0 用策略默认值
```

`invoke` 同步返回终态 `ExecutionRecord`；被拒绝时抛 `InvocationRejectedException`，
其 `getReason()` 给出可区分原因。

### REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/functions/{name}/invoke` | 触发函数。Header：`X-Idempotency-Key`（必填）、`X-Ordering-Key`、`X-Timeout-Millis`；Body 为 payload |
| GET | `/executions/{id}` | 按执行 ID 查询记录 |
| GET | `/executions?idempotencyKey=...` | 按幂等键查询记录 |
| GET | `/dead-letters` | 查看死信队列 |

拒绝时的 HTTP 状态码与原因一一对应：
`UNKNOWN_FUNCTION→404`、`INVALID_ARGUMENT→400`、`CONCURRENCY_LIMIT_EXCEEDED / QUEUE_FULL→429`，
响应体含 `reason` 字段。

## 执行状态机

```
            校验/查找失败
提交 ─────────────────────────► （抛 InvocationRejectedException，不落记录）
  │
  ▼
PENDING ──► RUNNING ──► SUCCEEDED                 （成功，终态）
              │
              ├──────► FAILED ──► RETRYING ──► RUNNING …（attempts < maxAttempts）
              │          │
              │          └──────► DEAD_LETTERED           （重试耗尽，终态，入死信队列）
              │
              └──────► TIMED_OUT                          （超时中断，终态，不重试）

执行前并发超限：PENDING ──► REJECTED（终态；未产生副作用，幂等键被释放，可重新提交）
```

每次状态流转都写入 `ExecutionStore` 并打印日志（幂等键、状态、重试次数）。

## 重试与幂等规则

- **重试**：函数抛异常视为失败，`attempts` 递增；未达 `maxAttempts` 则按
  `backoffMillis` 间隔重试；达到上限进入 `DEAD_LETTERED` 并加入死信队列
  （`FunctionRuntime.deadLetters()` / `GET /dead-letters`）。
- **超时不重试**：超时调用被中断（`Future.cancel(true)`）并直接进入 `TIMED_OUT`
  终态，避免中断后重试造成重复副作用；记录上不写入结果，迟到的函数返回值
  不得覆盖终态，不留半成品状态。
- **幂等**：`idempotencyKey` 是存储层的唯一索引（`putIfAbsent` 原子去重）。
  相同键的重复提交不会再次执行函数：若首次执行仍在途，重复提交方等待其完成，
  然后返回**同一条**执行记录——重试与重复提交的最终结论一致，副作用只发生一次。
- **拒绝不消耗幂等键**：被 `REJECTED` 的调用未产生任何副作用，其幂等键映射被移除，
  调用方修正后可用同一键重新提交。

## 并发与背压策略

- **限流**：公平信号量控制最大并发（`setConcurrencyLimit(max, queueWaitMillis)`）。
  `queueWaitMillis > 0` 时超限调用排队等待空位；等待超时或为 0 时以
  `CONCURRENCY_LIMIT_EXCEEDED` **明确拒绝**，绝不静默丢弃。
- **按键顺序**：每个非空 `orderingKey` 对应一个公平信号量（FIFO），同一键上的调用
  按提交顺序串行执行（含重试全程持锁）；不同键之间互不影响。
- **隔离**：每次调用拥有独立的 `ExecutionRecord`，函数体为无共享状态的处理器，
  同一函数的多次调用互不污染。

## 本地验证

```bash
mvn test
```

测试覆盖（`FunctionRuntimeTests`，日志打印幂等键、执行状态与重试次数）：

| 用例 | 验证点 |
|---|---|
| `unknownFunctionIsRejectedWithDistinguishableReason` | 未知函数 → `UNKNOWN_FUNCTION` |
| `invalidArgumentIsRejectedWithDistinguishableReason` | 非法入参 → `INVALID_ARGUMENT` |
| `duplicateSubmissionHasNoDuplicateSideEffectAndSameConclusion` | 并发重复提交：副作用 1 次、同一记录、结论一致 |
| `retryExhaustionGoesToDeadLetter` | 重试耗尽 → `DEAD_LETTERED`，attempts == maxAttempts，重复提交不再执行 |
| `retrySucceedsBeforeExhaustion` | 重试上限内成功 → `SUCCEEDED` |
| `concurrencyLimitExceededIsRejectedNotDropped` | 超限 → `CONCURRENCY_LIMIT_EXCEEDED`；释放后同键可重新提交成功 |
| `timeoutInterruptsAndLeavesNoPartialState` | 超时 → `TIMED_OUT`，函数被中断，无结果残留，终态稳定 |
| `sameOrderingKeyTakesEffectInSubmissionOrder` | 同键调用按提交顺序生效 |
| `invocationsOfSameFunctionDoNotPolluteEachOther` | 同函数多次调用状态隔离 |

运行服务：`mvn spring-boot:run` 后按上文 REST 约定调用。

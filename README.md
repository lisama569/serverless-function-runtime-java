# Serverless Function Runtime（本地可复现）

进程内的 Serverless 函数调用与持久化执行运行时：以内存存储与调度模拟函数注册、
按名触发、执行状态流转、失败重试、死信、死信重放、幂等去重、并发限流、
超时中断与按键顺序保证，以及死信容量治理（上限 + 最旧先淘汰 + 淘汰可观测）。

## 模块结构

```
runtime/FunctionRegistry      函数注册表（按名注册/查找）
runtime/FunctionRuntime       运行时核心：校验→幂等→排序→限流→超时执行→重试/死信→重放
runtime/ExecutionStore        进程内执行记录存储（executionId 主索引 + idempotencyKey 唯一索引）
runtime/RetryPolicy           可配置重试策略（maxAttempts / backoffMillis / defaultTimeoutMillis）
runtime/DemoFunctions         内置示例函数（demo-flaky / echo），启动即可走通全链路
model/ExecutionRecord         持久化执行记录（状态、重试次数、重放次数、最近重放结果、拒绝原因）
model/ExecutionStatus         执行状态机
model/RejectionReason         可区分的拒绝原因
model/ReplayOutcome           重放结论（NONE/SUCCEEDED/EXHAUSTED_AGAIN/TIMED_OUT/REJECTED）
model/DeadLetterEviction      死信容量淘汰事件（可观测、可解释）
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
| GET | `/dead-letters?functionName=...` | 查看未结案死信，可按函数名过滤；每条记录含 `replayCount` 与 `lastReplayOutcome` |
| POST | `/dead-letters/{executionId}/replay` | 重放一条死信（沿用原入参/顺序/超时/重试策略） |
| GET | `/dead-letters/evictions` | 死信容量淘汰事件（`totalEvicted` 累计数 + 最近事件列表） |

拒绝时的 HTTP 状态码与原因一一对应：
`UNKNOWN_FUNCTION / UNKNOWN_EXECUTION→404`、`INVALID_ARGUMENT→400`、
`NOT_REPLAYABLE→409`、`CONCURRENCY_LIMIT_EXCEEDED / QUEUE_FULL→429`，
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
              │          └──────► DEAD_LETTERED           （重试耗尽，入未结案死信）
              │                    │
              │                    │ replay（重放：重置 attempts，获得完整重试预算）
              │                    ▼
              │                 RUNNING ──► SUCCEEDED     （移出死信，终态）
              │                    │
              │                    └──────► DEAD_LETTERED （再次耗尽：回到死信，replayCount+1）
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

## 死信重放

`runtime.replay(executionId)` / `POST /dead-letters/{executionId}/replay`
对一条**未结案死信**发起一次重新执行：

- **沿用原始约束**：重放使用记录上的原 payload、`orderingKey`、`timeoutMillis`
  与当前重试策略；重放前 `attempts` 归零，本轮获得完整的 `maxAttempts` 重试预算。
- **结果流转**：重放成功 → `SUCCEEDED` 终态并从未结案死信中消失；
  再次耗尽重试 → 回到死信，`replayCount` 累计 +1，`lastReplayOutcome=EXHAUSTED_AGAIN`；
  重放中超时 → `TIMED_OUT` 终态（与正常调用一致，超时不重试）。
- **单在途重放**：同一条记录同一时刻只允许一次在途重放（按 `executionId` 单飞）。
  并发或重复的重放请求不会把函数体多跑一遍，而是等待在途重放并分享同一份结论。
- **可区分的拒绝**：记录不存在 → `UNKNOWN_EXECUTION`（404）；
  记录不在未结案死信中（已成功终结、被拒绝、或已被容量淘汰）→ `NOT_REPLAYABLE`（409）。
  两者都不会被悄悄当成成功。
- **不走旁路**：重放与新调用共用同一把顺序锁与同一份并发额度——
  不绕过并发上限（超限即 `CONCURRENCY_LIMIT_EXCEEDED` 明确拒绝，记录保持死信原状、
  不计重放次数），不打乱同一顺序键上按提交顺序生效的约束，也不堵塞不相关的调用。
- **与既有规则的兼容边界**：重放不改变幂等语义（原幂等键仍指向同一记录，
  重复提交不会再次执行）；不改变重试/超时/限流/顺序的既有结论；
  重放只是给死信记录一次「重新进入执行管线」的机会。

## 死信容量治理

- **上限可配**：`setDeadLetterCapacity(n)`（默认 1000）。死信只会无限增长的场景
  被容量上限约束；调低容量会立即触发淘汰。
- **最旧先淘汰**：超过上限时按进入死信的顺序淘汰最旧的记录。
- **淘汰可观测**：每次淘汰打印告警日志并落一条 `DeadLetterEviction` 事件
  （`GET /dead-letters/evictions` 查看累计数与最近事件）。
- **查询语义不乱**：被淘汰的记录仍保留在 `ExecutionStore` 中，按 `executionId`
  可查、状态保持 `DEAD_LETTERED`；但它不再属于未结案死信集合，
  不再出现在 `GET /dead-letters` 中，也不可再重放（`NOT_REPLAYABLE`）。

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

死信重放与容量治理（`DeadLetterReplayTests`，日志以 `scenario=` 开头标注场景）：

| 用例 | 验证点 |
|---|---|
| `replaySucceedsToTerminalStateAndLeavesDeadLetter` | 重放成功 → `SUCCEEDED` 终态、移出死信、`replayCount=1`；重复提交仍幂等 |
| `replayExhaustsAgainAndReturnsToDeadLetterWithCountAccumulated` | 重放再次耗尽 → 回到死信，重放次数累计，每轮有完整重试预算 |
| `concurrentAndDuplicateReplayExecutesOnlyOnceWithConsistentConclusion` | 并发/重复重放：函数体只执行一次，各方结论一致 |
| `replayOfNonDeadLetterOrUnknownRecordIsRejectedDistinguishably` | 已终结 → `NOT_REPLAYABLE`；不存在 → `UNKNOWN_EXECUTION`，两者可区分 |
| `replayRespectsConcurrencyLimitAndDoesNotBlockUnrelatedCalls` | 重放共用并发额度，超限明确拒绝且不改写记录；不堵不相关调用 |
| `replayRespectsOrderingKeySubmissionOrder` | 重放与同键新调用按提交顺序生效 |
| `deadLetterCapacityEvictsOldestWithObservableTrail` | 超容量最旧先淘汰，淘汰事件可观测；被淘汰记录可查但不可重放 |
| `deadLettersFilterByFunctionNameAndExposeReplayMetadata` | 死信按函数名过滤，可见 `replayCount` 与 `lastReplayOutcome` |

## 开箱验证整条链路

服务内置示例函数 `demo-flaky`（前 3 次调用必失败、第 4 次起成功），
启动后无需任何准备即可走通「按名触发 → 失败进入死信 → 重放成功」：

```bash
mvn spring-boot:run

# 1) 触发：默认 maxAttempts=3，三次尝试全部失败 -> DEAD_LETTERED
curl -X POST localhost:8080/functions/demo-flaky/invoke \
     -H 'X-Idempotency-Key: demo-1' -d 'hello'

# 2) 查看死信（可按 functionName 过滤），记下 executionId
curl 'localhost:8080/dead-letters?functionName=demo-flaky'

# 3) 重放：第 4 次调用成功 -> SUCCEEDED，并从未结案死信中消失
curl -X POST localhost:8080/dead-letters/<executionId>/replay
curl localhost:8080/dead-letters        # 应为空

# 4) 对已终结记录再重放 -> 409 NOT_REPLAYABLE（可区分的拒绝）
curl -X POST localhost:8080/dead-letters/<executionId>/replay
```

运行服务：`mvn spring-boot:run` 后按上文 REST 约定调用。

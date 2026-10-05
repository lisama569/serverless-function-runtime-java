# Serverless Function Runtime（本地可复现）

进程内的 Serverless 函数调用与持久化执行运行时：以内存存储与调度模拟函数注册、
按名触发、执行状态流转、失败重试、死信、幂等去重、并发限流、超时中断中断与按键顺序保证。

## 模块结构

```
runtime/FunctionRegistry      函数注册表（按名注册/查找）
runtime/FunctionRuntime       运行时核心：校验→幂等→排序→限流→超时执行→重试/死信→死信重放
runtime/ExecutionStore        进程内执行记录存储（executionId 主索引 + idempotencyKey 唯一索引）
runtime/RetryPolicy           可配置重试策略（maxAttempts / backoffMillis / defaultTimeoutMillis）
model/ExecutionRecord         持久化执行记录（状态、重试次数、重放次数、结果、拒绝原因）
model/ExecutionStatus         执行状态机
model/RejectionReason         可区分的拒绝原因
web/FunctionController        REST 入口
SampleFunctions               内置示例函数（echo / demo-flaky），启动即可验证完整链路
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
| GET | `/dead-letters?functionName=...` | 查看死信队列，可按函数名过滤；每条记录含 `replayCount` 与 `lastReplayOutcome` |
| POST | `/dead-letters/{executionId}/replay` | 重放一条死信（见下文「死信重放」） |

拒绝时的 HTTP 状态码与原因一一对应：
`UNKNOWN_FUNCTION→404`、`INVALID_ARGUMENT→400`、`CONCURRENCY_LIMIT_EXCEEDED / QUEUE_FULL→429`、
`REPLAY_NOT_DEAD_LETTERED→404`、`REPLAY_ALREADY_TERMINAL→409`，
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
              │                     │
              │                     │ replay() 重放
              │                     ▼
              │                  RUNNING ──► SUCCEEDED    （重放成功，移出死信）
              │                     │
              │                     ├──────► DEAD_LETTERED（重放再次耗尽，回到死信，replayCount+1）
              │                     │
              │                     └──────► TIMED_OUT    （重放超时，终态结案，移出死信，不得再重放）
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

## 死信重放

进入死信的调用可以按执行标识重新跑起来，而不是只能躺着看：

```java
runtime.replay(executionId);   // 或 POST /dead-letters/{executionId}/replay
```

- **沿用原参数**：重放重建原始请求，沿用原入参、顺序键、超时与当前重试策略，
  重新走完整条执行管线（排序 → 限流 → 超时执行 → 重试/死信）。
- **成功即结案**：重放成功进入 `SUCCEEDED` 终态，并从未结案死信列表中移除。
- **超时同样结案**：重放以超时结束（`TIMED_OUT`）时按终态处理——超时本来就是终态，
  记录会从未结案死信列表中移除、按终结结案，不会出现「终态记录仍挂在死信里」的
  自相矛盾状态。此后对它再发起重放会被 `REPLAY_ALREADY_TERMINAL`（HTTP 409）
  可区分地拒绝，函数体不会再跑一遍、`replayCount` 也不再增加。
  （注意：从未进过死信的超时记录被重放时仍得到 `REPLAY_NOT_DEAD_LETTERED`，语义不变。）
- **再耗尽回死信**：重放再次耗尽重试则回到 `DEAD_LETTERED` 并留在死信中；
  记录上的 `replayCount` 累计重放次数，`lastReplayOutcome` 记录最近一次重放的结果，
  死信列表与执行记录查询均可见。
- **在途去重**：同一条记录同一时刻只允许一次在途重放。并发或重复触发的重放
  等待首个重放得出结论并返回**同一条**记录——函数体不会多跑一遍，各方结论一致。
- **可区分的拒绝**：
  - 目标已终结（`SUCCEEDED`，或重放以 `TIMED_OUT` 终态结案）→ `REPLAY_ALREADY_TERMINAL`（HTTP 409）；
  - 目标不存在或不在死信中（含已被淘汰、从未进死信的超时终态等）→ `REPLAY_NOT_DEAD_LETTERED`（HTTP 404）；
  - 重放时并发额度不足 → `CONCURRENCY_LIMIT_EXCEEDED`（HTTP 429），
    记录保持 `DEAD_LETTERED` 且留在死信中、不计重放次数，可稍后再次重放。
    以上拒绝都不会被悄悄当成成功。

### 与既有规则的兼容边界

- **不走旁路**：重放与新调用共用同一套并发额度与按键顺序锁——既不绕过并发上限，
  也在同一 `orderingKey` 上按提交顺序排队生效；只占用自己的额度与顺序位，
  不会堵住不相关的调用。
- **幂等不变**：重放复用原执行记录与幂等键映射，不产生新记录；
  重复提交同一幂等键仍然命中同一条记录（包括重放后的终态结论）。
- **重试/超时语义不变**：重放的一轮执行遵守与首次调用相同的重试上限、退避与
  「超时即终态不重试」规则；`replayCount` 只统计真正拿到额度开始执行的重放，
  被限流拒绝的重放不计数。
- **既有结论不变**：重复提交去重、重试耗尽进死信、超限明确拒绝、超时中断的
  行为与重放能力互不影响（见回归测试 `FunctionRuntimeTests`）。

## 死信容量与淘汰

- 死信队列有容量上限（默认 1000，`setDeadLetterCapacity(capacity)` 可配），
  防止长时间运行后无限堆积。
- 超过上限时**最旧的先淘汰**。每次淘汰都会打日志（被汰记录的 executionId、
  幂等键、当前容量），并累计到 `deadLetterEvictions()` 计数，淘汰行为可观测、可解释。
- **并发写入下上限严格成立**：「入队 + 超限淘汰」在同一把锁内原子完成，
  只有确实超过上限时才淘汰最旧的一条；未超限的记录不会因为并发写入被提前挤出去。
  因此任意时刻都有 `死信数 ≤ 容量`，且 `淘汰计数 == 累计入队数 - 当前死信数`。
- 淘汰只把记录移出「未结案死信列表」，**不删除执行记录**：仍可按执行 ID / 幂等键
  查询到该记录（状态保持 `DEAD_LETTERED`），查询语义不变；对已被淘汰的记录发起
  重放会得到 `REPLAY_NOT_DEAD_LETTERED` 的明确拒绝。

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

死信重放覆盖（`DeadLetterReplayTests`，日志打印幂等键、状态、重放次数与最近重放结果）：

| 用例 | 验证点 |
|---|---|
| `replaySucceedsMovesToTerminalAndLeavesDeadLetter` | 重放成功 → `SUCCEEDED`，移出死信，`replayCount=1` |
| `replayReExhaustionReturnsToDeadLetterAndCountsReplays` | 重放再耗尽 → 回死信，重放次数累计，无重复条目 |
| `replayEndingWithTimeoutIsTerminalAndFurtherReplayRejected` | 重放超时 → `TIMED_OUT` 终态结案、移出死信；再重放 → `REPLAY_ALREADY_TERMINAL`，函数体不再执行 |
| `concurrentReplayExecutesBodyOnceWithSameConclusion` | 并发重放：函数体只多跑一次，各方结论一致 |
| `replayOfNonDeadLetterOrTerminalIsRejectedDistinguishably` | 不存在/非死信 → `REPLAY_NOT_DEAD_LETTERED`；已终结 → `REPLAY_ALREADY_TERMINAL` |
| `replayRespectsConcurrencyLimitAndKeepsRecordInDeadLetters` | 额度不足 → 明确拒绝，记录留死信、不计重放次数、不执行函数体 |
| `replayTakesEffectInSubmissionOrderOnSameOrderingKey` | 重放与正常调用在同一顺序键上按提交顺序生效 |
| `deadLetterCapacityEvictsOldestObservably` | 超容量最旧先淘汰，淘汰计数可观测，记录仍可查询，重放被拒可区分 |
| `concurrentDeadLetterWritesNeverEvictBelowCapacity` | 并发写入未超限：一条不淘汰、计数为 0 |
| `concurrentDeadLetterWritesRespectCapacityStrictly` | 高并发反复写入：死信数严格等于容量上限，淘汰数 == 入队数 - 容量 |
| `deadLettersCanBeFilteredByFunctionName` | 死信列表按函数名过滤 |

### 开箱即用的端到端链路

服务内置示例函数（`SampleFunctions`）：`echo` 与 `demo-flaky`
（前 3 次尝试必失败、第 4 次起成功，默认重试策略下首次触发恰好耗尽进死信）。
启动后无需任何准备即可跑通「触发 → 死信 → 重放成功」：

```bash
mvn spring-boot:run

# 1. 触发：3 次尝试全部失败，进入死信
curl -X POST localhost:8080/functions/demo-flaky/invoke -H 'X-Idempotency-Key: demo-1' -d 'hello'
#    → status=DEAD_LETTERED, attempts=3

# 2. 查看死信（可按 functionName 过滤）
curl 'localhost:8080/dead-letters?functionName=demo-flaky'

# 3. 重放：第 4 次调用成功，移出死信
curl -X POST localhost:8080/dead-letters/{executionId}/replay
#    → status=SUCCEEDED, replayCount=1, lastReplayOutcome=SUCCEEDED

# 4. 对已终结记录重放：可区分的拒绝
curl -X POST localhost:8080/dead-letters/{executionId}/replay
#    → HTTP 409, reason=REPLAY_ALREADY_TERMINAL
```

运行服务：`mvn spring-boot:run` 后按上文 REST 约定调用。

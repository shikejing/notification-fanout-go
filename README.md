# notification-fanout-go

通知扇出与投递调度子系统（Go）。单个上游事件按规则分发到一个或多个下游
通道；各通道可用性、时延与失败模式相互独立。整个投递过程**可解释、可恢复、
可重复验证**，下游通道用**内存通道**与**本地回环 HTTP 通道**模拟。

- 模块路径：`github.com/shikejing/notification-fanout-go`
- 包：`fanout`（无第三方依赖，仅标准库）

## 快速开始

```go
eng, _ := fanout.NewEngine(fanout.EngineConfig{
    StorePath: "/var/lib/notify/wal.log", // 空串表示仅内存
})
eng.AddChannel(emailCh, fanout.ChannelConfig{Name: "email", MaxAttempts: 3})
eng.AddRule(fanout.RoutingRule{EventType: "order", Channel: "email"})
eng.Start(ctx)

eng.Submit(ctx, fanout.Event{ID: "order-1", Type: "order", Payload: body})
rep, _ := eng.WaitForEvent(ctx, "order-1") // 等待全部通道到达终态
```

测试与示例使用 `FakeClock`（手动推进时间），因此调度、退避、超时都是确定性、
可重复的。可运行示例见 `fanout/example_test.go`。

## 核心抽象

| 概念 | 类型 | 说明 |
| --- | --- | --- |
| 事件 | `Event` | 上游通知。`ID` 是逻辑通知的**幂等键**；`NotBefore` 用于定时投递 |
| 路由规则 | `RoutingRule` | 按 `EventType` 把事件映射到通道；同一事件去重后按通道名排序 |
| 通道 | `Channel` | 下游目标，`Send(ctx, deliveryID, payload)` 必须按 `deliveryID` 幂等 |
| 投递 | `Delivery` | 一个 `(事件, 通道)` 的完整状态机 + 每次尝试历史 |
| 存储 | `Store` | 只追加 WAL（`FileStore`）或内存实现（`MemoryStore`） |
| 调度器 | `Dispatcher` | 每个通道一个：并发度、速率上限、NotBefore 定时 |
| 时钟 | `Clock` | `RealClock` / `FakeClock`，决定调度与退避行为 |

## 投递状态机（可解释）

每个 `(event, channel)` 投递有以下状态，所有迁移都带可读原因：

```
        created
           |
           v
       pending ──(disabled)──────────────► skipped   （终态）
           |
           v
       inflight ──(2xx / nil)────────────► success   （终态）
           |
     尝试失败但仍有额度 ──(backoff)──► pending
           |
     尝试次数耗尽 ───────────────────► failed    （终态，进入死信队列）
```

- 结果三分类（`ResultCode`）：`success` / `failed` / `skipped`。
- `skipped` 的标准原因：`dedup: logical notification already accepted`、
  `channel disabled in configuration`、`trigger fired before not-before time`。
- `failed` 的原因为 `max attempts exhausted (N); last error: <下游错误>`，
  且 `Delivery.Attempts[]` 记录每次尝试的开始/结束时间与具体错误，失败链路
  可一直追溯到根因（注入失败、超时 deadline、网络错误等）。
- 查询：`Engine.Report(eventID)`、`Engine.Delivery(eventID, channel)`、
  `Engine.DeadLetters()`。

## 投递语义（重要）

**有效投递：“每逻辑通知 × 每通道，下游副作用恰好一次”（exactly-once side
effect）。** 它由三层共同保证：

1. **入口去重（at-most-once acceptance）**
   - 事件被接受时先持久化一条 `accepted` WAL 记录，`Event.ID` 重复进入
     直接返回既有报告，不再产生任何投递。
   - 因此“调度触发晚到”（事件成功后再次触发/提交）不会造成二次投递。

2. **投递幂等键**
   - 每个通道的投递使用稳定 ID：`deliveryID = eventID + "#" + channelName`。
   - 该 ID 作为 `MemoryChannel` 的去重键，以及回环 HTTP 的
     `Idempotency-Key` 请求头；通道/服务端对同一键只产生一次副作用。

3. **崩溃安全的状态边界（at-least-once 执行 + 幂等收敛）**
   - 每次尝试在**开始前**与**结束后**各写一条 WAL（fsync）。
   - 若进程在尝试中途崩溃，重放只会看到“开始”而无“结束”：恢复时把该尝试
     视为被中断并按退避重试；由于下游按键幂等，重放的重复 `Send` 不会产生
     第二个副作用，状态最终收敛为恰好一次。

### 定时投递与“晚到触发”

- `Event.NotBefore` 之前投递只排队不执行。
- 若投递已经到达终态（成功/失败/跳过），任何迟到的调度触发或重复提交都被
  状态机拒绝（`isTerminal` 短路 + 入口去重），不会重复投递。
- 重启后未触发的定时投递依据 WAL 中的 `NotBefore` 继续推进。

### 取消与超时

- 每次尝试可由 `AttemptTimeout` 限定；超时与关停产生的取消都会让 `Send`
  返回错误，尝试结果被如实记录。
- 取消发生在“下游已确认之前”：投递停留在 `pending`（而不是某个中间态），
  由恢复逻辑继续推进，**不消耗成功语义、也不丢失任务**。
- 关停时调度器取消在途任务并返回队列任务；这些任务无需在内存中重新入队，
  因为其状态已在 WAL 中，重启时由重放重新派生队列。

### 重试、退避与死信

- `MaxAttempts` 为最大尝试次数（`1` 表示不重试）。
- 第 n 次失败后等待 `InitialBackoff × 2^(n-1)`，上限为 `MaxBackoff`。
- 尝试耗尽 → 状态 `failed`，写入 `terminal` + `dead` 两条记录，进入死信
  队列；`DeadLetters()` 返回完整尝试历史与最终原因。

### 并发与速率（排队，不丢弃）

- 每个通道独立 `Dispatcher`：`Concurrency` 个工作位 + 令牌桶 `RatePerSecond`。
- 超过并发/速率的投递进入**最早到期优先**的最小堆排队，等待令牌/空位，
  **绝不丢弃**；速率等待严格 FIFO。
- 通道之间完全隔离：一个通道阻塞或持续失败不会影响其它通道的投递进度。

## 持久化与恢复（WAL）

- `FileStore` 是单行 JSON、只追加的日志；每次 `Append` 都 `fsync` 后才返回。
- 条目类型：`accepted / created / attempt(start|finish) / terminal / dead`。
  `created` 条目中持久化了 payload，因此重放后无需上游即可重新投递。
- `Start` 时完整重放重建内存状态，随后：
  - 终态（success/failed/skipped）不再调度；
  - `inflight`（崩溃时在途）按“已中断尝试 + 退避”重新调度；
  - `pending` 取 `NotBefore` 与“当前时间+剩余退避”中较晚者继续。
- 日志不可变，每条状态变更都有持久化原因，因此恢复前后状态自洽、可逐条解释。

## 配置项说明（`ChannelConfig`）

| 字段 | 含义 | 默认/约束 |
| --- | --- | --- |
| `Name` | 通道唯一名，被路由规则引用 | 必填（空则取 `Channel.Name()`） |
| `Kind` | 实现类型：`memory` / `loopback` | 仅用于说明/观测 |
| `Disabled` | 为 true 时路由到该通道的投递直接 `skipped`，无副作用 | false |
| `Concurrency` | 该通道最大在途投递数；超出排队 | `<=0` 视为 1 |
| `RatePerSecond` | 每秒最多开始的尝试数（令牌桶，突发容量 1）；超限排队 | `<=0` 不限速 |
| `MaxAttempts` | 单投递最大尝试次数；耗尽进死信 | `<=0` 视为 1 |
| `InitialBackoff` | 第 2 次尝试前的初始退避 | 0 表示立即重试 |
| `MaxBackoff` | 指数退避上限 | 0 表示不设上限 |
| `AttemptTimeout` | 单次尝试超时；超时按失败处理并可重试 | 0 表示不超时 |

## 下游通道

- **MemoryChannel**（`Kind=memory`）：进程内通道，可配置 `Latency`、
  `FailFirst`（前 N 次新尝试失败）、`BlockOnIDs`（阻塞直到 `Unblock`，用于
  模拟挂死下游）；按 `deliveryID` 保证仅一次副作用。
- **LoopbackChannel**（`Kind=loopback`）：仅允许回环地址（127.0.0.1/::1/
  localhost，构造时校验）。POST 投递并带 `Idempotency-Key`。配套
  `LoopbackServer` 是可编程的本地 HTTP 服务器（延迟、前 N 次 500、阻塞、
  服务端幂等），用于在真实 loopback 网络路径上验证失败与重放。

## 测试覆盖

`go test ./... -race`（测试全部使用确定性的 FakeClock / loopback）：

- 重复输入去重、幂等返回、重复提交零额外副作用（`dedup_test.go`）
- NotBefore 定时、到期前不投递、晚到触发不重复、禁用通道跳过
- 指数退避后成功、最大尝试次数与死信、失败原因可追溯（`retry_test.go`）
- 单通道失败/阻塞不影响其它通道（`isolation_test.go`、`retry_test.go`）
- 尝试超时重试、并发度排队不丢弃、速率上限 FIFO、关停无中间态
  （`concurrency_test.go`）
- 崩溃恢复：已提交不重复、在途续跑、多次重启收敛、死信恢复、定时任务恢复
  （`recovery_test.go`）
- 回环 HTTP：真实 loopback 网络失败/重试/服务端幂等、非回环地址拒绝、
  端到端接入引擎（`loopback_test.go`）

## 代码结构

```
types.go           领域类型与状态/结果枚举
clock.go           Clock/Timer：RealClock + 可确定性推进的 FakeClock
store_file.go      JSON-lines 只追加 WAL（fsync），重放恢复
store_memory.go    内存 Store（测试用）
store_codec.go     Entry <-> JSON 编解码
channel_memory.go  幂等内存通道（延迟/失败/阻塞注入）
channel_loopback.go 仅回环的 HTTP 通道 + 可编程测试服务器
ratelimit.go       令牌桶，严格 FIFO 排队
dispatcher.go      每通道调度器：最小堆定时 + 并发 + 速率 + 取消
engine.go          引擎：注册、Submit 扇出、WAL 重放与状态重建
engine_process.go  投递状态机：尝试/退避/死信/跳过/恢复重排
engine_query.go    Report/WaitForEvent/DeadLetters/Shutdown
*_test.go          场景化自动化测试
```

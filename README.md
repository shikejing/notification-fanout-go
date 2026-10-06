# notification-fanout-go

通知扇出与投递调度子系统。上游产生的单个事件按规则扇出到一个或多个下游通道，
各通道独立推进；整个投递生命周期可解释、可恢复、可重复验证。
下游通道用内存实现（`SimChannel`）模拟，真实通道只需实现 `Channel` 接口。

## 架构

```
 upstream Event
      │
      ▼
 Dispatcher ── dedup(按事件 ID 幂等) ──► Store(JSONL 追加日志, 先持久化后生效)
      │ 为每个 (事件, 通道) 创建 Delivery
      ▼
 Scheduler(最小堆定时器)  ──到期──►  guarded 状态迁移 pending→queued
      │                                (晚到/重复触发 → 记为 skipped, 无副作用)
      ▼
 channelRunner(每通道独立)
   ├─ 队列(超限排队, 不丢弃, 满则背压)
   ├─ 令牌桶限速(RatePerSec)
   ├─ worker 池(Concurrency)
   ├─ 超时控制(DeliverTimeout)
   └─ 失败 → 指数退避重试 → 超过 MaxAttempts → 死信(Dead Letter)
```

## 投递语义

- **扇出隔离**：每个 `(eventID, channel)` 是一个独立的 `Delivery`，有自己的状态机、
  重试预算与审计日志。任一通道失败或阻塞不影响其它通道。
- **恰好一次的可观察副作用**：以 `Event.ID` 为幂等键去重；同一逻辑通知重复提交是
  无操作（返回 `Duplicated: true`）。崩溃恢复时 `in_flight` 投递会被重置为 `queued`
  重新投递，因此要求通道按 `Delivery.ID` 幂等（`SimChannel` 即如此），
  下游观察到的副作用恰好一次。
- **调度不重复**：定时触发只做一次受守护的状态迁移 `pending → queued`。
  晚到或重复的触发被记录为 `skipped` 尝试（含可读原因），不会再次投递。
- **结果可区分**：每次尝试记录为 `Attempt{Outcome, Reason}`，
  `Outcome ∈ {success, failure, skipped}`，原因人类可读；
  完整审计链可通过 `Dispatcher.Attempts(deliveryID)` 查询。
- **崩溃恢复**：所有状态迁移先写 JSONL 日志并 fsync 后生效。重启后
  `OpenFileStore` 回放日志重建状态，`Dispatcher.Recover`：
  - `succeeded/dead/canceled`（终态）：不再触碰，已提交的投递绝不重复；
  - `in_flight`：记一条 `skipped` 审计（"recovered after restart"）后重置为 `queued`；
  - `queued`：重新入队；`pending`：按原 `NotBefore` 重新调度。
- **取消与超时不留中间态**：`Cancel` 将非终态投递置为 `canceled` 并撤销调度；
  worker 关闭打断的尝试记为 `skipped` 并回到 `queued`，不消耗重试预算；
  单次尝试超时（`DeliverTimeout`）按普通失败进入退避重试。

## 状态机

```
pending ──到期──► queued ──领取──► in_flight ──成功──► succeeded
  ▲                 ▲                 │
  │                 │                 ├─失败且可重试──► pending(退避后重新调度)
  │                 │                 ├─失败且耗尽/永久错误──► dead(死信)
  │                 └────中断(interrupt)┘
  └── Cancel ──► canceled（任意非终态可取消）
```

## 配置项（ChannelConfig）

| 字段 | 含义 | 默认值 |
|---|---|---|
| `Concurrency` | 该通道最大并发在途投递数 | 1 |
| `RatePerSec` | 每秒最多发起的投递数（令牌桶，容量 1）；0 = 不限速 | 0 |
| `MaxAttempts` | 单个投递的最大尝试次数，耗尽进入死信 | 3 |
| `BaseBackoff` | 首次重试延迟，之后指数翻倍 | 50ms |
| `MaxBackoff` | 重试延迟上限 | 5s |
| `DeliverTimeout` | 单次尝试超时；0 = 不限制 | 0 |
| `QueueCapacity` | 排队容量；0 = 无界。满时入队阻塞（背压），投递绝不丢弃 | 0 |

## API 概览

```go
store, _ := notify.OpenFileStore("data/store.jsonl")
d := notify.NewDispatcher(store)
d.RegisterChannel(notify.NewSimChannel("email"), notify.ChannelConfig{
    Concurrency: 4, RatePerSec: 20, MaxAttempts: 5,
})
d.Recover(ctx) // 重启后恢复未完成的调度

res, _ := d.Submit(ctx, notify.Event{
    ID: "order-123",              // 幂等键
    Payload: []byte(`{...}`),
    Targets: []string{"email", "webhook"},
    NotBefore: time.Now().Add(time.Hour), // 可选：定时投递
})
res.Duplicated // true 表示重复提交，已忽略

d.Delivery(ctx, id)   // 查询投递状态
d.Attempts(ctx, id)   // 查询审计链（成功/失败/跳过 + 原因）
d.DeadLetters(ctx)    // 查询死信
d.Cancel(ctx, id)     // 取消未完成的投递
d.Close()
```

## 持久化格式

`FileStore` 使用追加式 JSONL 日志（`event` / `delivery` / `attempt` 三类记录），
每次写入后 fsync。重启时整日志回放，最后一条记录生效；
崩溃造成的残缺尾行在回放时被忽略（该记录从未被确认）。

## 测试

```sh
go test -race ./...
```

覆盖：扇出隔离、重复输入幂等、定时与晚到触发、重试与退避、死信与永久错误、
并发上限、速率上限、尝试超时与取消、重启恢复（含 in_flight 中断恢复）。

# 🌊 MoonFlow

> 基于 MoonBit 的确定性工作流引擎 — 崩溃可恢复、执行可重放、步骤可重试

[![MoonBit](https://img.shields.io/badge/MoonBit-v0.10-blue)](https://www.moonbitlang.com/)
[![Tests](https://img.shields.io/badge/tests-42%2F42-green)](./)
[![License](https://img.shields.io/badge/license-Apache%202.0-orange)](./LICENSE)

---

## 一句话介绍

MoonFlow 是一个 **确定性工作流执行引擎**。你用声明式 DSL 定义工作流步骤，引擎负责调度执行。进程崩溃？重放事件日志，从断点继续，不丢状态，不重复执行。

## 为什么需要它

```
传统写法：手工状态机，崩溃后从头再来
  let status = "pending"
  if status == "pending" { do_step1(); status = "step1_done" }
  if status == "step1_done" { do_step2(); status = "step2_done" }
  // 💥 崩溃！status 丢失，step1 重复执行，数据错乱

MoonFlow：声明式 DSL，崩溃后从断点恢复
  workflow("order")
    .then("step1", ...)
    .then("step2", ...)
    .then("step3", ...)
  // 💥 崩溃 → resume_workflow(id) → 自动跳过 step1, step2，继续 step3
```

## 三大核心能力

| 能力 | 说明 |
|------|------|
| 🔄 **崩溃恢复** | 所有状态变化记录为事件日志，进程重启后自动从断点继续执行 |
| 🎬 **确定性重放** | `replay_events()` 纯函数 — 同一份事件日志永远重建相同状态，便于调试和审计 |
| 📝 **声明式 DSL** | 4 行代码定义完整工作流：`workflow().then().parallel().branch().build()` |

## 30 秒快速上手

```moonbit
// 1. 创建引擎
let engine = @moonflow.new_memory_engine()

// 2. 注册步骤
engine.register("hello", fn(input) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
  println("Hello World!")
  @moonflow.Async::pure(Ok(input))
})

// 3. 定义工作流
let wf = @moonflow.workflow("my-first-wf")
  .then("hello", @moonflow.StepConfig::default("hello"))
  .build()

// 4. 执行！
let result = @moonflow.Async::run(engine.start(wf, Json::null()))
```

## 功能矩阵

```
┌────────────────────────────────────────────────────────────┐
│  🏗️ 工作流 DSL                                             │
│  ├─ .then(step, config)        顺序步骤                    │
│  ├─ .parallel(group, [...])    并行步骤组                  │
│  └─ .branch(name, cond, T, F)  条件分支                    │
├────────────────────────────────────────────────────────────┤
│  ⚙️ 执行引擎                                               │
│  ├─ Engine::start()            启动工作流                  │
│  ├─ Engine::resume_workflow()  崩溃恢复 (跳过已完成→继续)  │
│  ├─ 自动重试                   max_retry + 递归执行        │
│  └─ 超时检测                   timeout_ms + StepTimeout    │
├────────────────────────────────────────────────────────────┤
│  💾 持久化                                                 │
│  ├─ EventLog                   不可变追加事件日志          │
│  ├─ Storage trait              Async 存储接口              │
│  ├─ MemoryStorage              内存 (测试用)               │
│  └─ FileStorage                文件 (持久化)               │
├────────────────────────────────────────────────────────────┤
│  🔍 运维能力                                               │
│  ├─ 工作流验证器               validate_workflow()         │
│  ├─ 查询 API                   query_status/summary        │
│  ├─ 结果 API                   get_result/error            │
│  ├─ 补偿回滚                   补偿计划 + 执行             │
│  ├─ 中间件                     logging/retry/circuit-breaker│
│  └─ 指标监控                   42 项指标 + JSON 导出       │
└────────────────────────────────────────────────────────────┘
```

## 示例：电商订单工作流

```moonbit
let engine = @moonflow.new_memory_engine()

// 注册5个步骤 (含补偿步骤)
engine.register("validate_order",   validate_handler)
engine.register("reserve_stock",    reserve_handler)
engine.register("process_payment",  payment_handler)
engine.register("send_confirmation",email_handler)
engine.register("release_stock",    compensate_handler)  // 失败时释放库存

// 声明式定义
let wf = @moonflow.workflow("order-processing")
  .then("validate_order",   StepConfig::default("validate_order"))
  .then("reserve_stock",    StepConfig::with_retry("reserve_stock", 5))
  .then("process_payment",  StepConfig::with_timeout("process_payment", 10000))
  .then("send_confirmation",StepConfig::default("send_confirmation"))
  .build()

let result = @moonflow.Async::run(engine.start(wf, input))

// 💥 模拟崩溃后恢复
let engine2 = @moonflow.new_memory_engine()
// ... 重新注册 handlers ...
engine2.resume_workflow(wf_id, wf)  // 自动跳过已完成步骤
```

## 示例：ETL 数据管道 (含并行)

```moonbit
let wf = @moonflow.workflow("etl-pipeline")
  .then("extract_csv",      StepConfig::default("extract_csv"))
  .then("validate_schema",  StepConfig::default("validate_schema"))
  // 👇 三个转换步骤并行执行
  .parallel("transform_all", [
    "transform_users",
    "transform_orders",
    "transform_products"
  ])
  .then("load_database",    StepConfig::with_retry("load_database", 3))
  .then("send_report",      StepConfig::default("send_report"))
  .build()
```

## MoonBit 特性运用

| 特性 | 运用 |
|------|------|
| **ADT 枚举** | `WorkflowEvent` (7 事件)、`WorkflowError` (7 错误)、`StepType` (3 类型) |
| **Trait 系统** | `Storage`、`TimerProvider` — 可热替换后端 |
| **泛型 + 约束** | `Engine[S : Storage]` — 编译期类型安全 |
| **Result 类型** | 零 panic，全部可恢复错误 |
| **模式匹配** | 引擎调度基于穷尽模式匹配 |
| **WASM 编译** | 可嵌入浏览器 / 边缘环境 |

## 项目状态

```
✅ moon build    — 0 errors
✅ moon test     — 42/42 passed
✅ moon run examples/order_workflow
✅ moon run examples/etl_pipeline
📦 moonbitlang/async@0.19.3
```

## 安装

```bash
moon add moonbitlang/moonflow
```

## 运行示例

```bash
moon run examples/order_workflow    # 电商订单流程
moon run examples/etl_pipeline      # ETL 数据管道
```

## 运行测试

```bash
moon test    # 42 tests, 0 failures
```

## 文档

- [设计文档 (DESIGN.md)](./moonflow/DESIGN.md) — 架构决策与技术细节
- [项目申报书 (PROPOSAL.md)](./moonflow/PROPOSAL.md) — 项目背景、技术方案、创新点

## License

Apache-2.0
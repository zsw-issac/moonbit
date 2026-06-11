# MoonFlow 项目申报书

## — 基于 MoonBit 的确定性工作流执行引擎

---

## 一、项目概述

### 1.1 项目名称

**MoonFlow** — MoonBit Deterministic Workflow Engine

### 1.2 项目定位

对标 Temporal / Durable Task Framework / AWS Step Functions，面向 **确定性模拟 (Deterministic Simulation)** 和 **异步并发工作流** 场景，提供一个轻量级、可嵌入、崩溃可恢复的工作流执行引擎。

### 1.3 核心技术栈

| 维度 | 选型 |
|------|------|
| 编程语言 | MoonBit (最新稳定版) |
| 包管理 | moon (MoonBit 官方工具链) |
| 异步运行时 | `moonbitlang/async@0.19.3` |
| 标准库 | `moonbitlang/core` |
| 编译目标 | WASM / Native |

---

## 二、项目背景与意义

### 2.1 问题陈述

在分布式系统和长时间运行的业务流程中，以下问题普遍存在：

1. **进程崩溃导致状态丢失**：传统命令式工作流在执行中途崩溃后必须从头重新执行，导致已完成的步骤（如扣款、发货）被重复执行或数据不一致
2. **工作流调试困难**：异步执行的工作流难以追踪每一步的输入输出，出问题时无法回溯
3. **手写状态机复杂易错**：开发者被迫手动管理状态转换、重试逻辑、超时处理，代码冗长且 bug 频发

### 2.2 解决方案

MoonFlow 通过 **事件溯源 (Event Sourcing)** 和 **确定性重放 (Deterministic Replay)** 解决上述问题：

- 所有状态变化记录为不可变事件日志
- 进程崩溃后从事件日志重建状态，从断点继续执行
- 工作流可完整重放，便于调试和审计
- 提供声明式 DSL，开发者只需描述步骤依赖，引擎负责调度

### 2.3 MoonBit 特性契合点

本项目的技术选型充分发挥了 MoonBit 语言的独特优势：

| 特性 | 应用 |
|------|------|
| **ADT (代数数据类型)** | `WorkflowEvent`、`WorkflowError`、`StepType` 等核心类型使用枚举表达状态机，无 null 无异常 |
| **Trait 系统** | `Storage` trait 支持内存/文件/数据库后端热替换，`TimerProvider` 支持时间模拟测试 |
| **泛型 + Trait 约束** | `Engine[S]` 配合 `fn[S : Storage]` 实现类型安全的存储抽象 |
| **Result/Option** | 全部可失败操作返回 `Result[T, WorkflowError]`，零 panic |
| **模式匹配** | 引擎调度和事件重放基于 ADT 穷尽模式匹配，编译期保证分支完整性 |
| **WASM 编译** | 引擎可编译到 WASM 在浏览器/边缘环境运行 |

---

## 三、技术方案

### 3.1 核心架构

```
                    ┌─────────────────┐
                    │  WorkflowBuilder │  ← 声明式 DSL
                    │  .then().branch()│
                    │  .parallel()     │
                    └────────┬────────┘
                             │ WorkflowDef (DAG)
                             ▼
┌──────────┐     ┌───────────────────────┐     ┌──────────────┐
│  Storage │◄────│        Engine         │────►│   Registry   │
│  trait   │     │  start / resume       │     │ step → handler│
└────┬─────┘     │  execute_step / retry │     └──────────────┘
     │           └───────────┬───────────┘
     │                       │ 事件写入
     ▼                       ▼
┌──────────┐     ┌───────────────────────┐
│MemoryStore│    │      EventLog         │  ← 不可变追加
│FileStore  │    │  WorkflowStarted      │
└──────────┘    │  StepCompleted ...     │
                └───────────┬───────────┘
                            │ replay_events() ← 纯函数
                            ▼
                ┌───────────────────────┐
                │ WorkflowExecutionState │  ← 确定性重建
                │ completed_steps       │
                │ current_step          │
                └───────────────────────┘
```

### 3.2 事件溯源 (Event Sourcing)

7 种事件类型覆盖完整生命周期：

```
WorkflowStarted ──► StepStarted ──► StepCompleted ──► ... ──► WorkflowCompleted
                        │                                           │
                        └── StepRetrying ──► StepFailed ──► WorkflowFailed
```

### 3.3 崩溃恢复流程

```
1. 进程崩溃
2. 重启 → Engine::resume_workflow(id)
3. Storage::load_events(id) → 事件日志
4. replay_events(events) → WorkflowExecutionState (确定性)
5. 遍历 WorkflowDef.steps
   ├─ 步骤名在 state.completed_steps 中 → 跳过，使用缓存输出
   └─ 步骤名不在 → 执行 handler
6. 工作流完成
```

### 3.4 关键技术指标

| 指标 | 值 |
|------|-----|
| 步骤最大重试次数 | 可配置 (默认 3) |
| 步骤超时 | 可配置 (默认 30s) |
| 重试策略 | 固定延迟 / 指数退避 |
| 并行步骤 | 支持 (Parallel StepType) |
| 条件分支 | 支持 (Conditional StepType) |
| 存储后端 | Memory / File (可扩展) |

---

## 四、实现内容

### 4.1 代码规模

| 类别 | 文件数 | 代码行数 |
|------|--------|----------|
| 核心引擎 (core) | 8 | ~700 |
| 状态管理 (state) | 4 | ~250 |
| 类型定义 (types) | 2 | ~55 |
| 运行时 (runtime) | 3 | ~120 |
| API & 工具 | 5 | ~300 |
| 中间件 & 策略 | 3 | ~230 |
| 示例 (examples) | 2 | ~190 |
| 测试 (42 个用例) | 4 | ~700 |
| **总计** | **31** | **~2,550** |

### 4.2 功能清单

- ✅ 事件溯源 — 7 种 WorkflowEvent 类型
- ✅ 确定性重放 — `replay_events()` 纯函数
- ✅ 崩溃恢复 — `resume_workflow()` 跳过已完成步骤并继续执行
- ✅ 声明式 DSL — `workflow().then().parallel().branch().build()`
- ✅ 步骤重试 — 可配置 max_retry + 递归重试逻辑
- ✅ 超时检测 — 壁钟超时 + `StepTimeout` 错误
- ✅ 并行步骤组 — `Parallel` StepType
- ✅ 条件分支 — `Conditional` StepType
- ✅ 存储抽象 — `Storage` trait (返回 `Async[Result[...]]`)
- ✅ MemoryStorage / FileStorage
- ✅ `moonbitlang/async` 集成
- ✅ 快照序列化 — `WorkflowSnapshot::to_json/from_json`
- ✅ 工作流验证 — `validate_workflow()`
- ✅ 查询 API — `query_status()`, `query_summary()`, `list_all_workflows()`
- ✅ 结果 API — `get_result()`, `get_error()`
- ✅ 补偿回滚 — `build_compensation_plan()`, `execute_compensation()`
- ✅ 中间件系统 — logging / retry / circuit-breaker
- ✅ 重试策略 — Fixed / ExponentialBackoff / NoRetry
- ✅ 指标监控 — `WorkflowMetrics` (支持 JSON 导出)
- ✅ 42 个测试用例 (T01-T42)

### 4.3 测试覆盖

```
T01-T05   引擎基础 (顺序执行/重试/超时/并行)
T06-T10   确定性重放
T11-T13   快照序列化
T14-T20   并行失败/条件分支/超时触发/大工作流/多并行组
T21-T24   重试回放/失败回放/100事件压力/零重试
T25-T30   唯一性/存储列表/配置参数/构建器步数/验证器
T31-T33   快照内容/结果查询/错误查询
T34-T42   崩溃恢复/并行恢复/条件恢复/新引擎重启/EventLog不可变
```

---

## 五、示例演示

### Demo 1：电商订单工作流

```moonbit
let engine = @moonflow.new_memory_engine()
engine.register("validate_order", handler_validate)
engine.register("reserve_stock", handler_reserve)
engine.register("process_payment", handler_payment)
engine.register("send_confirmation", handler_email)
engine.register("release_stock", handler_compensate)  // 补偿步骤

let wf = @moonflow.workflow("order-processing")
  .then("validate_order", StepConfig::default(...))
  .then("reserve_stock", StepConfig::with_retry(..., 5))
  .then("process_payment", StepConfig::with_timeout(..., 10000))
  .then("send_confirmation", StepConfig::default(...))
  .build()

let result = @moonflow.Async::run(engine.start(wf, input))
// 模拟崩溃 → 新引擎 → resume:
engine2.resume_workflow(wf_id, wf)  // 跳过已完成步骤
```

### Demo 2：ETL 数据管道（含并行步骤）

```moonbit
let wf = @moonflow.workflow("etl-pipeline")
  .then("extract_csv", ...)
  .then("validate_schema", ...)
  .parallel("transform_all", ["transform_users", "transform_orders", "transform_products"])
  .then("load_database", StepConfig::with_retry(..., 3))
  .then("send_report", ...)
  .build()
```

---

## 六、创新点与优势

1. **MoonBit 首创**：首个基于 MoonBit 语言的确定性工作流引擎
2. **事件溯源 + 确定重放**：崩溃恢复不是"最大努力"而是数学保证
3. **声明式 DSL**：4 行代码定义完整工作流 DAG
4. **可嵌入**：单文件库，无需外部服务，可编译到 WASM 在边缘运行
5. **完整测试**：42 个测试覆盖正常路径、异常路径、恢复路径、边界条件

---

## 七、项目文件结构

```
moonflow/
├── lib.mbt                      # 公开入口
├── types_workflow_id.mbt        # WorkflowId 类型
├── types_errors.mbt             # WorkflowError 枚举
├── state_event_log.mbt          # 事件类型 + 不可变日志
├── state_storage.mbt            # Storage trait (Async)
├── state_replay.mbt             # 确定性重放引擎
├── state_snapshot.mbt           # 快照序列化
├── core_step.mbt                # StepConfig / StepNode / StepType
├── core_workflow.mbt            # WorkflowBuilder DSL
├── core_engine.mbt              # 执行引擎 (start/resume/execute)
├── core_scheduler.mbt           # Async[T] 封装
├── core_policy.mbt              # 重试策略 / 选项
├── core_validator.mbt           # 工作流验证器
├── core_middleware.mbt          # 中间件系统
├── core_compensation.mbt        # 补偿回滚
├── runtime_memory_storage.mbt   # 内存存储
├── runtime_file_storage.mbt     # 文件存储
├── runtime_timer.mbt            # 定时器抽象
├── runtime_context.mbt          # 执行上下文
├── api_query.mbt                # 查询 API
├── api_result.mbt               # 结果 API
├── monitor_metrics.mbt          # 指标监控
├── utils_time.mbt               # 时间工具
├── examples/order_workflow/     # Demo 1
├── examples/etl_pipeline/       # Demo 2
├── *_wbtest.mbt (4个文件)       # 42 个白色盒测试
├── README.md                    # 项目说明
├── DESIGN.md                    # 设计文档
└── moon.mod                     # 模块配置
```

---

## 八、总结

MoonFlow 是一个功能完整的确定性工作流引擎，核心创新在于将 **Event Sourcing + Deterministic Replay** 引入 MoonBit 生态。项目实现了声明式工作流 DSL、崩溃恢复、并行执行、条件分支、中间件、指标监控等完备功能，42 个测试全部通过，两个 Demo 可运行展示完整流程。

项目代码精简（~2,550 行）但架构完整，充分发挥了 MoonBit 的 ADT、Trait、泛型、模式匹配等语言特性，可作为 MoonBit 生态中工作流/确定性模拟领域的参考实现。

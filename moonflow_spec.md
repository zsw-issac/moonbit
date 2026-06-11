# MoonFlow — MoonBit Deterministic Workflow Engine
## 完整项目规格文档 (Agent 执行版)

---

## 一、项目定位

**项目名称**: `moonflow`  
**对标参考**: Temporal / Durable Task Framework / AWS Step Functions  
**官方清单对应**: `Deterministic simulation 框架` + `基于 moonbitlang/async 做的并发框架`  
**目标代码量**: 5000~7000 有效行 MoonBit 代码  
**语言版本**: 最新稳定版 MoonBit  
**包管理**: moon (MoonBit 官方工具链)

---

## 二、核心价值主张

MoonFlow 提供一个**确定性工作流执行引擎**，解决以下问题：

1. 工作流执行过程中进程崩溃后能从断点恢复，无需重头执行
2. 工作流的每次执行均可完整重放（replay），便于调试和审计
3. 工作流步骤支持异步执行、超时、重试，基于 `moonbitlang/async`
4. 提供声明式的工作流定义 API，而非命令式的"手写状态机"

**MoonBit 特性契合点**:
- 利用 MoonBit 的 `Result`/`Option` 类型做错误处理，无需异常
- 利用 `moonbitlang/async` 做异步 step 调度
- 利用 MoonBit 的 ADT (Algebraic Data Types) 表达工作流状态机
- 编译到 WASM 后可在浏览器/边缘环境运行

---

## 三、项目目录结构

```
moonflow/
├── moon.mod.json              # 模块配置
├── README.md                  # 项目说明
├── DESIGN.md                  # 设计文档
│
├── src/
│   ├── core/                  # 核心引擎 (~1800 行)
│   │   ├── workflow.mbt       # Workflow 定义与 DSL
│   │   ├── step.mbt           # Step 抽象与执行
│   │   ├── engine.mbt         # 执行引擎主循环
│   │   └── scheduler.mbt      # 异步调度器
│   │
│   ├── state/                 # 状态管理 (~1200 行)
│   │   ├── event_log.mbt      # 事件日志 (核心持久化结构)
│   │   ├── snapshot.mbt       # 状态快照序列化/反序列化
│   │   ├── replay.mbt         # 重放引擎
│   │   └── storage.mbt        # 存储抽象接口
│   │
│   ├── types/                 # 公共类型 (~600 行)
│   │   ├── result.mbt         # 扩展 Result 类型
│   │   ├── workflow_id.mbt    # ID 类型
│   │   └── errors.mbt         # 错误类型定义
│   │
│   ├── runtime/               # 运行时适配 (~800 行)
│   │   ├── memory_storage.mbt # 内存存储实现
│   │   ├── timer.mbt          # 超时与定时器
│   │   └── context.mbt        # 执行上下文
│   │
│   └── lib.mbt                # 公开 API 导出
│
├── examples/
│   ├── order_workflow/        # Demo 1: 电商订单流
│   │   └── main.mbt
│   └── etl_pipeline/          # Demo 2: 数据 ETL 管道
│       └── main.mbt
│
└── test/
    ├── engine_test.mbt
    ├── replay_test.mbt
    └── snapshot_test.mbt
```

---

## 四、数据类型设计

### 4.1 工作流核心类型

```moonbit
// src/types/workflow_id.mbt

pub struct WorkflowId {
  value : String
}

pub fn WorkflowId::new(s : String) -> WorkflowId {
  { value: s }
}

pub fn WorkflowId::generate() -> WorkflowId {
  // 生成唯一 ID，格式: "wf-{timestamp}-{random}"
  { value: "wf-\{@time.now()}-\{@random.int()}" }
}
```

```moonbit
// src/types/errors.mbt

pub enum WorkflowError {
  StepFailed(step_name : String, reason : String)
  StepTimeout(step_name : String, timeout_ms : Int)
  StepMaxRetriesExceeded(step_name : String, attempts : Int)
  WorkflowNotFound(id : WorkflowId)
  InvalidTransition(from : String, to : String)
  SerializationError(msg : String)
  StorageError(msg : String)
}
```

```moonbit
// src/core/step.mbt

// Step 执行结果
pub enum StepResult[T] {
  Success(value : T)
  Failure(error : WorkflowError)
  Pending                        // 异步步骤尚未完成
}

// Step 配置
pub struct StepConfig {
  name       : String
  max_retry  : Int               // 最大重试次数，默认 3
  timeout_ms : Int               // 超时毫秒数，0 表示不限
  retry_delay_ms : Int           // 重试等待毫秒数
}

pub fn StepConfig::default(name : String) -> StepConfig {
  { name, max_retry: 3, timeout_ms: 30000, retry_delay_ms: 1000 }
}

// Step 抽象：一个可执行的工作单元
pub struct Step[I, O] {
  config  : StepConfig
  execute : (I) -> Async[StepResult[O]]   // 使用 moonbitlang/async
}
```

### 4.2 工作流定义类型

```moonbit
// src/core/workflow.mbt

// 工作流状态枚举
pub enum WorkflowStatus {
  Pending       // 等待执行
  Running       // 执行中
  Completed     // 成功完成
  Failed        // 执行失败
  Cancelled     // 被取消
}

// 工作流实例
pub struct WorkflowInstance {
  id          : WorkflowId
  name        : String
  status      : WorkflowStatus
  created_at  : Int64            // Unix timestamp ms
  updated_at  : Int64
  current_step : String
  result      : Json?            // 最终输出，序列化为 JSON
  error       : WorkflowError?
}

// 工作流定义 (DAG)
pub struct WorkflowDef[I, O] {
  name  : String
  steps : Array[StepNode]        // 有序步骤节点
  // 步骤之间的依赖关系 (step_name -> depends_on_step_names)
  deps  : Map[String, Array[String]]
}

// DAG 节点
pub struct StepNode {
  name       : String
  config     : StepConfig
  step_type  : StepType
}

pub enum StepType {
  Sequential                     // 顺序执行
  Parallel(steps : Array[String]) // 并行执行一组步骤
  Conditional(                   // 条件分支
    condition : String,          // 条件表达式名称
    on_true   : String,          // 满足时跳转到的步骤名
    on_false  : String
  )
}
```

### 4.3 事件日志类型（持久化核心）

```moonbit
// src/state/event_log.mbt

// 事件类型枚举：所有状态变化都记录为事件
pub enum WorkflowEvent {
  WorkflowStarted(
    workflow_id : WorkflowId,
    name        : String,
    input       : Json,
    timestamp   : Int64
  )
  StepStarted(
    workflow_id : WorkflowId,
    step_name   : String,
    attempt     : Int,
    timestamp   : Int64
  )
  StepCompleted(
    workflow_id : WorkflowId,
    step_name   : String,
    output      : Json,
    timestamp   : Int64
  )
  StepFailed(
    workflow_id : WorkflowId,
    step_name   : String,
    error       : String,
    attempt     : Int,
    timestamp   : Int64
  )
  StepRetrying(
    workflow_id : WorkflowId,
    step_name   : String,
    attempt     : Int,
    delay_ms    : Int,
    timestamp   : Int64
  )
  WorkflowCompleted(
    workflow_id : WorkflowId,
    output      : Json,
    timestamp   : Int64
  )
  WorkflowFailed(
    workflow_id : WorkflowId,
    error       : String,
    timestamp   : Int64
  )
}

// 事件日志：不可变追加
pub struct EventLog {
  workflow_id : WorkflowId
  events      : Array[WorkflowEvent]  // 按时间顺序追加
}

pub fn EventLog::new(id : WorkflowId) -> EventLog {
  { workflow_id: id, events: [] }
}

pub fn EventLog::append(
  self : EventLog,
  event : WorkflowEvent
) -> EventLog {
  // 返回新的 EventLog（不可变追加）
  let new_events = self.events.copy()
  new_events.push(event)
  { ..self, events: new_events }
}
```

### 4.4 存储抽象接口

```moonbit
// src/state/storage.mbt

// 存储接口：支持不同后端（内存、文件、数据库）
pub trait Storage {
  // 保存事件到日志
  save_event(Self, WorkflowId, WorkflowEvent) -> Async[Result[Unit, WorkflowError]]

  // 读取某个 workflow 的完整事件日志
  load_events(Self, WorkflowId) -> Async[Result[Array[WorkflowEvent], WorkflowError]]

  // 保存快照（可选优化）
  save_snapshot(Self, WorkflowId, WorkflowSnapshot) -> Async[Result[Unit, WorkflowError]]

  // 读取最新快照
  load_snapshot(Self, WorkflowId) -> Async[Result[WorkflowSnapshot?, WorkflowError]]

  // 列出所有 workflow 实例
  list_workflows(Self) -> Async[Result[Array[WorkflowId], WorkflowError]]
}
```

---

## 五、核心引擎实现

### 5.1 执行引擎主逻辑

```moonbit
// src/core/engine.mbt

pub struct Engine[S : Storage] {
  storage  : S
  registry : Map[String, StepExecutor]   // step_name -> 执行函数
}

// 步骤执行器类型
pub type StepExecutor = (Json) -> Async[Result[Json, WorkflowError]]

pub fn Engine::new[S : Storage](storage : S) -> Engine[S] {
  { storage, registry: Map::new() }
}

// 注册一个具体步骤的执行逻辑
pub fn Engine::register[S : Storage](
  self    : Engine[S],
  name    : String,
  handler : StepExecutor
) -> Unit {
  self.registry[name] = handler
}

// 启动一个工作流实例
pub fn Engine::start[S : Storage](
  self        : Engine[S],
  workflow_def : WorkflowDef,
  input       : Json
) -> Async[Result[WorkflowId, WorkflowError]] {
  // 实现逻辑：
  // 1. 生成 WorkflowId
  // 2. 写入 WorkflowStarted 事件
  // 3. 调用 execute_workflow 开始执行
  // 4. 返回 WorkflowId（不等待执行完成）
  ...
}

// 恢复一个已有工作流（崩溃恢复入口）
pub fn Engine::resume[S : Storage](
  self : Engine[S],
  id   : WorkflowId
) -> Async[Result[Unit, WorkflowError]] {
  // 实现逻辑：
  // 1. 从 storage 读取事件日志
  // 2. 通过 replay 重建当前状态
  // 3. 找到最后执行到的步骤
  // 4. 从该步骤继续执行（已完成的步骤直接跳过）
  ...
}

// 内部：执行工作流 DAG
fn Engine::execute_workflow[S : Storage](
  self         : Engine[S],
  workflow_id  : WorkflowId,
  workflow_def : WorkflowDef,
  state        : WorkflowExecutionState
) -> Async[Result[Json, WorkflowError]] {
  // 实现逻辑：
  // 1. 遍历 workflow_def.steps 中的节点
  // 2. 根据 StepType 分别处理 Sequential / Parallel / Conditional
  // 3. 每次执行前检查 state.completed_steps，已完成的直接用缓存结果
  // 4. 调用 execute_step 执行单个步骤
  ...
}

// 内部：执行单个步骤（含重试逻辑）
fn Engine::execute_step[S : Storage](
  self        : Engine[S],
  workflow_id : WorkflowId,
  node        : StepNode,
  input       : Json,
  attempt     : Int
) -> Async[Result[Json, WorkflowError]] {
  // 实现逻辑：
  // 1. 写入 StepStarted 事件
  // 2. 查找 registry 中的 handler
  // 3. 带超时执行 handler(input)
  // 4. 成功：写入 StepCompleted 事件，返回 output
  // 5. 失败且 attempt < max_retry：写入 StepRetrying 事件，等待 retry_delay，递归调用
  // 6. 失败且超过重试：写入 StepFailed 事件，返回 Error
  ...
}
```

### 5.2 重放引擎

```moonbit
// src/state/replay.mbt

// 执行状态（由事件日志重建）
pub struct WorkflowExecutionState {
  workflow_id     : WorkflowId
  status          : WorkflowStatus
  completed_steps : Map[String, Json]  // step_name -> output（已完成的步骤结果）
  failed_steps    : Map[String, WorkflowError]
  current_step    : String
  step_attempts   : Map[String, Int]   // step_name -> 已尝试次数
}

// 从事件日志重建执行状态（纯函数，无副作用）
pub fn replay_events(
  events : Array[WorkflowEvent]
) -> Result[WorkflowExecutionState, WorkflowError] {
  // 实现逻辑：
  // 1. 从空状态开始，遍历 events
  // 2. 对每个事件类型做状态转移：
  //    - WorkflowStarted  -> 初始化状态
  //    - StepStarted      -> 更新 current_step，增加 attempt 计数
  //    - StepCompleted    -> 加入 completed_steps
  //    - StepFailed       -> 加入 failed_steps
  //    - WorkflowCompleted -> status = Completed
  //    - WorkflowFailed   -> status = Failed
  // 3. 返回重建后的状态
  //
  // 关键性质：对同一份事件日志，该函数每次调用返回相同结果（确定性）
  ...
}
```

### 5.3 快照序列化

```moonbit
// src/state/snapshot.mbt

pub struct WorkflowSnapshot {
  workflow_id : WorkflowId
  state       : WorkflowExecutionState
  event_count : Int     // 快照时的事件数量（用于增量重放）
  created_at  : Int64
}

// 将执行状态序列化为 JSON（用于持久化）
pub fn WorkflowSnapshot::to_json(
  self : WorkflowSnapshot
) -> Json {
  // 序列化 WorkflowExecutionState 中所有字段
  ...
}

// 从 JSON 反序列化恢复快照
pub fn WorkflowSnapshot::from_json(
  json : Json
) -> Result[WorkflowSnapshot, WorkflowError] {
  // 反序列化，字段缺失或类型错误返回 SerializationError
  ...
}
```

---

## 六、内存存储实现

```moonbit
// src/runtime/memory_storage.mbt
// 用于测试和单机运行的内存存储

pub struct MemoryStorage {
  // workflow_id -> 事件列表
  event_logs : Map[String, Array[WorkflowEvent]]
  // workflow_id -> 最新快照
  snapshots  : Map[String, WorkflowSnapshot]
}

pub fn MemoryStorage::new() -> MemoryStorage {
  { event_logs: Map::new(), snapshots: Map::new() }
}

// 实现 Storage trait
impl Storage for MemoryStorage with save_event(self, id, event) {
  // 追加事件到对应 workflow 的事件列表
  ...
}

impl Storage for MemoryStorage with load_events(self, id) {
  // 返回对应 workflow 的事件列表副本
  ...
}

impl Storage for MemoryStorage with save_snapshot(self, id, snapshot) {
  ...
}

impl Storage for MemoryStorage with load_snapshot(self, id) {
  ...
}

impl Storage for MemoryStorage with list_workflows(self) {
  ...
}
```

---

## 七、公开 API 设计

```moonbit
// src/lib.mbt
// 这是用户使用 moonflow 的入口

// 创建引擎（内存模式，适合测试）
pub fn new_memory_engine() -> Engine[MemoryStorage] {
  Engine::new(MemoryStorage::new())
}

// 构建工作流定义的 Builder API
pub struct WorkflowBuilder {
  name  : String
  steps : Array[StepNode]
  deps  : Map[String, Array[String]]
}

pub fn workflow(name : String) -> WorkflowBuilder {
  { name, steps: [], deps: Map::new() }
}

// 链式添加顺序步骤
pub fn WorkflowBuilder::then(
  self   : WorkflowBuilder,
  name   : String,
  config : StepConfig
) -> WorkflowBuilder {
  ...
}

// 链式添加并行步骤组
pub fn WorkflowBuilder::parallel(
  self       : WorkflowBuilder,
  group_name : String,
  step_names : Array[String]
) -> WorkflowBuilder {
  ...
}

// 链式添加条件分支
pub fn WorkflowBuilder::branch(
  self       : WorkflowBuilder,
  name       : String,
  condition  : String,
  on_true    : String,
  on_false   : String
) -> WorkflowBuilder {
  ...
}

// 构建最终 WorkflowDef
pub fn WorkflowBuilder::build(
  self : WorkflowBuilder
) -> WorkflowDef {
  ...
}
```

---

## 八、Demo 示例

### Demo 1：电商订单工作流

```moonbit
// examples/order_workflow/main.mbt

fn main {
  let engine = @moonflow.new_memory_engine()

  // 注册步骤处理函数
  engine.register("validate_order", fn(input) {
    // 验证订单数据
    // 返回 Ok(validated_order_json) 或 Err(...)
    ...
  })

  engine.register("reserve_stock", fn(input) {
    // 调用库存系统预留库存
    ...
  })

  engine.register("process_payment", fn(input) {
    // 调用支付系统
    ...
  })

  engine.register("send_confirmation", fn(input) {
    // 发送确认邮件/消息
    ...
  })

  engine.register("release_stock", fn(input) {
    // 支付失败时释放库存（补偿步骤）
    ...
  })

  // 定义工作流 DAG
  let order_workflow = @moonflow.workflow("order-processing")
    .then("validate_order",    StepConfig::default("validate_order"))
    .then("reserve_stock",     StepConfig::{ ..StepConfig::default("reserve_stock"), max_retry: 5 })
    .then("process_payment",   StepConfig::{ ..StepConfig::default("process_payment"), timeout_ms: 10000 })
    .then("send_confirmation", StepConfig::default("send_confirmation"))
    .build()

  // 启动一个订单工作流实例
  let input = Json::object([
    ("order_id",   Json::string("ORD-12345")),
    ("user_id",    Json::string("USR-001")),
    ("items",      Json::array([...])),
    ("total_amount", Json::number(299.0))
  ])

  let result = engine.start(order_workflow, input) |> @async.run
  println("Workflow started: \{result}")

  // 模拟崩溃恢复：读取 workflow_id，重新 resume
  match result {
    Ok(wf_id) => {
      // 假设这里程序崩溃重启，从存储中恢复
      let resume_result = engine.resume(wf_id) |> @async.run
      println("Resumed: \{resume_result}")
    }
    Err(e) => println("Error: \{e}")
  }
}
```

### Demo 2：ETL 数据管道

```moonbit
// examples/etl_pipeline/main.mbt

fn main {
  let engine = @moonflow.new_memory_engine()

  engine.register("extract_csv",     fn(input) { ... })
  engine.register("validate_schema", fn(input) { ... })
  // 并行执行的三个转换步骤
  engine.register("transform_users",    fn(input) { ... })
  engine.register("transform_orders",   fn(input) { ... })
  engine.register("transform_products", fn(input) { ... })
  engine.register("load_database",   fn(input) { ... })
  engine.register("send_report",     fn(input) { ... })

  let etl_workflow = @moonflow.workflow("etl-pipeline")
    .then("extract_csv",     StepConfig::default("extract_csv"))
    .then("validate_schema", StepConfig::default("validate_schema"))
    // 三个 transform 步骤并行执行
    .parallel("transform_all", [
      "transform_users",
      "transform_orders",
      "transform_products"
    ])
    .then("load_database", StepConfig::{ ..StepConfig::default("load_database"), max_retry: 3 })
    .then("send_report",   StepConfig::default("send_report"))
    .build()

  let result = engine.start(etl_workflow, Json::object([
    ("source_file", Json::string("data/raw.csv")),
    ("target_db",   Json::string("postgres://..."))
  ])) |> @async.run

  println("ETL pipeline result: \{result}")
}
```

---

## 九、测试规格

### 9.1 引擎测试

```moonbit
// test/engine_test.mbt

// 测试用例列表（每个 test 块独立）：

// T01: 简单顺序工作流正常执行完成
// 验证：3 个顺序步骤全部执行，最终 status = Completed

// T02: 步骤失败触发重试
// 验证：step 第 1、2 次返回 Err，第 3 次返回 Ok
// 期望：workflow 最终 Completed，事件日志包含 2 个 StepRetrying 事件

// T03: 步骤超过最大重试次数
// 验证：step 始终返回 Err，max_retry = 3
// 期望：workflow 状态为 Failed，事件日志包含 StepMaxRetriesExceeded

// T04: 超时触发
// 验证：step 执行时间 > timeout_ms
// 期望：产生 StepTimeout 错误，触发重试逻辑

// T05: 并行步骤全部完成后继续
// 验证：parallel 组中 3 个步骤并发执行
// 期望：所有 3 个 StepCompleted 事件出现后，下一个步骤才开始
```

### 9.2 重放测试

```moonbit
// test/replay_test.mbt

// T06: 从空事件日志重建初始状态
// T07: 从完整事件日志重建 Completed 状态
// T08: 从中途中断的事件日志重建正确的当前步骤
// T09: 重放后 completed_steps 包含所有已完成步骤的输出
// T10: 对同一事件日志多次重放结果一致（确定性验证）
```

### 9.3 快照测试

```moonbit
// test/snapshot_test.mbt

// T11: WorkflowExecutionState 序列化后可完整反序列化
// T12: 快照辅助重放：事件数量较多时，从快照 + 增量事件重放比全量重放快
// T13: 快照版本兼容性（字段增减时的处理）
```

---

## 十、moon.mod.json 配置

```json
{
  "name": "moonbitlang/moonflow",
  "version": "0.1.0",
  "description": "A deterministic workflow engine for MoonBit with crash recovery and replay",
  "license": "Apache-2.0",
  "keywords": ["workflow", "async", "deterministic", "replay", "durable"],
  "repository": "https://github.com/moonbitlang/moonflow",
  "deps": {
    "moonbitlang/async": "0.1.0",
    "moonbitlang/core": "0.1.0"
  }
}
```

---

## 十一、README 核心内容（给 agent 生成）

README 需包含以下章节，agent 按此大纲撰写：

1. **项目简介**：一句话 + 三个核心特性（确定性执行 / 崩溃恢复 / 重放调试）
2. **安装方式**：`moon add moonbitlang/moonflow`
3. **快速上手**：最简单的 3 步顺序工作流代码示例（20 行以内）
4. **核心概念**：WorkflowDef、Step、EventLog、Replay 各一段简短说明
5. **API 参考**：列出所有公开函数签名及说明
6. **Demo 说明**：指向 examples/ 目录，说明两个 demo 的运行方式
7. **设计原则**：说明"确定性"的含义，为什么 EventLog 是核心，为什么不用可变状态

---

## 十二、实现顺序（Agent 执行建议）

按以下顺序实现，每一步可独立验证：

```
Step 1: 搭建项目骨架
  - 创建 moon.mod.json 和目录结构
  - 创建所有 .mbt 文件（空实现）
  - 确认 moon build 无报错

Step 2: 实现 types/ 下所有类型定义
  - WorkflowId, WorkflowError, StepConfig, StepResult
  - WorkflowStatus, WorkflowInstance
  - WorkflowEvent (最重要，所有事件类型)

Step 3: 实现 EventLog 和 MemoryStorage
  - EventLog 的 append / 查询方法
  - MemoryStorage 实现 Storage trait

Step 4: 实现 replay.mbt
  - replay_events 纯函数
  - 配合 T06-T10 测试用例验证

Step 5: 实现 WorkflowBuilder 和 WorkflowDef
  - workflow() / .then() / .parallel() / .branch() / .build()

Step 6: 实现 Engine 核心逻辑
  - Engine::start (仅顺序步骤)
  - Engine::execute_step (含重试)
  - 跑通 T01-T04 测试

Step 7: 实现并行步骤支持
  - execute_workflow 中处理 Parallel StepType
  - 跑通 T05 测试

Step 8: 实现 Engine::resume（崩溃恢复）
  - 调用 replay_events 重建状态
  - 从 current_step 继续执行

Step 9: 实现 snapshot.mbt
  - to_json / from_json
  - 跑通 T11-T13 测试

Step 10: 完成两个 Demo
  - examples/order_workflow/main.mbt
  - examples/etl_pipeline/main.mbt
  - 确保 moon run examples/... 可以运行并输出结果

Step 11: 补全 README 和 DESIGN.md
```

---

## 十三、评分亮点检查清单

Agent 完成后，确认以下内容存在：

- [ ] `replay_events` 函数是纯函数，有注释说明确定性保证
- [ ] `EventLog` 使用不可变追加（返回新 EventLog 而非修改原有）
- [ ] `Engine::resume` 有完整注释说明崩溃恢复流程
- [ ] 两个 Demo 的 `main` 函数中有模拟崩溃恢复的代码段
- [ ] `Storage` 是 trait，不是具体类型（体现可扩展性）
- [ ] 所有错误处理使用 `Result`，无 panic
- [ ] `moonbitlang/async` 用于实际的异步步骤执行
- [ ] `moon test` 全部通过（T01-T13）
- [ ] 有效 MoonBit 代码行数在 5000-7000 范围内
- [ ] README 明确说明"MoonBit 特性契合点"

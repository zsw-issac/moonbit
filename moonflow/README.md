# MoonFlow — Deterministic Workflow Engine for MoonBit

MoonFlow is a **deterministic workflow execution engine** built in MoonBit, providing:

- **Crash Recovery** — workflows resume from the last completed step after process restart
- **Deterministic Replay** — every execution can be fully replayed from the event log for debugging and auditing
- **Async & Retry Support** — steps support async execution, configurable timeouts and automatic retries
- **Declarative DSL** — chainable API to define workflow DAGs with sequential, parallel, and conditional steps

## Installation

```bash
moon add moonbitlang/moonflow
```

## Quick Start

```moonbit
fn main {
  // Create engine with in-memory storage
  let engine = @moonflow.Engine::new(@moonflow.MemoryStorage::new())

  // Register step handlers
  engine.register("hello", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("Hello from step!")
    @moonflow.Async::pure(Ok(input))
  })

  engine.register("world", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("World from step!")
    @moonflow.Async::pure(Ok(input))
  })

  // Define workflow
  let wf = @moonflow.workflow("hello-world")
    .then("hello", @moonflow.StepConfig::default("hello"))
    .then("world", @moonflow.StepConfig::with_retry("world", 3))
    .build()

  // Start execution
  let result = @moonflow.Async::run(engine.start(wf, Json::null()))
  println("Result: \{result}")
}
```

## Core Concepts

### WorkflowDef
A WorkflowDef describes the structure of a workflow — a DAG of **StepNodes**. It is built using the `WorkflowBuilder` chainable API that supports `.then()`, `.parallel()`, and `.branch()`.

### EventLog / Event Sourcing
All state transitions (step started, step completed, workflow failed, etc.) are recorded as **immutable WorkflowEvents**. This event log is the single source of truth — the execution state is reconstructed from it via deterministic replay.

### Replay
The `replay_events()` function is a **pure function** that takes an event log and produces the current `WorkflowExecutionState`. For identical inputs it always produces identical outputs — enabling audit trails and debugging.

### Storage Trait
The `Storage` trait provides an abstract interface for persistence backends. `MemoryStorage` is included for testing; other implementations (file, database) can be added.

## Project Structure

```
moonflow/
├── moon.mod
├── moon.pkg
├── types_workflow_id.mbt     # WorkflowId type
├── types_errors.mbt          # WorkflowError enum
├── state_event_log.mbt       # WorkflowEvent + EventLog
├── state_storage.mbt         # Storage trait
├── state_replay.mbt          # Replay engine
├── state_snapshot.mbt        # Snapshot serialization
├── core_step.mbt             # Step config + node types
├── core_workflow.mbt         # Workflow builder DSL
├── core_engine.mbt           # Execution engine
├── core_scheduler.mbt        # Async abstraction
├── runtime_memory_storage.mbt# In-memory storage
├── runtime_timer.mbt         # Timer abstraction
├── runtime_context.mbt       # Execution context
├── examples/
│   ├── order_workflow/       # E-commerce order pipeline
│   └── etl_pipeline/         # Data ETL with parallel steps
└── *_wbtest.mbt              # White-box tests (T01-T13)
```

## API Reference

### Engine
```moonbit
pub fn[S : Storage] Engine::new(storage : S) -> Engine[S]
pub fn[S] Engine::register(self, name : String, handler : StepExecutor) -> Unit
pub fn[S : Storage] Engine::start(self, wf : WorkflowDef, input : Json) -> Async[Result[WorkflowId, WorkflowError]]
pub fn[S : Storage] Engine::resume_workflow(self, id : WorkflowId) -> Async[Result[Unit, WorkflowError]]
```

### WorkflowBuilder
```moonbit
pub fn workflow(name : String) -> WorkflowBuilder
pub fn WorkflowBuilder::then(self, name : String, config : StepConfig) -> WorkflowBuilder
pub fn WorkflowBuilder::parallel(self, group_name : String, step_names : Array[String]) -> WorkflowBuilder
pub fn WorkflowBuilder::branch(self, name : String, condition : String, on_true : String, on_false : String) -> WorkflowBuilder
pub fn WorkflowBuilder::build(self) -> WorkflowDef
```

### StepConfig
```moonbit
pub fn StepConfig::default(name : String) -> StepConfig
pub fn StepConfig::with_retry(name : String, max_retry : Int) -> StepConfig
pub fn StepConfig::with_timeout(name : String, timeout_ms : Int) -> StepConfig
pub fn StepConfig::with_all(name : String, max_retry : Int, timeout_ms : Int, retry_delay_ms : Int) -> StepConfig
```

### Replay
```moonbit
pub fn replay_events(events : Array[WorkflowEvent]) -> Result[WorkflowExecutionState, WorkflowError]
```

## Design Principles

1. **Determinism** — Given the same event log, `replay_events()` always returns the same state. This makes debugging and auditing trivial.

2. **Event Sourcing** — The event log IS the data. Snapshots are an optimization, never the source of truth.

3. **Trait-based Storage** — The `Storage` trait enables swapping backends without changing the engine.

4. **No mutable global state** — The engine's state is explicitly passed through event logs, not mutated in place.

## Running Demos

```bash
# Order processing workflow
moon run examples/order_workflow

# ETL pipeline
moon run examples/etl_pipeline
```

## Testing

```bash
moon test
```

42 tests covering: sequential execution, retry logic, max retries exceeded, parallel steps, conditional branching, timeout enforcement, replay reconstruction, interrupted recovery, crash recovery, snapshot serialization, event log immutability, workflow validation, retry policies, middleware, and metrics.

## MoonBit Feature Usage

MoonFlow leverages key MoonBit language features to build a safe, expressive workflow engine:

| Feature | Usage |
|---------|-------|
| **ADTs (Algebraic Data Types)** | `WorkflowEvent`, `WorkflowError`, `WorkflowStatus`, `StepType` use enum types to represent state machines cleanly without nulls or exceptions |
| **Traits for Extensibility** | `Storage` trait enables swapping backends (MemoryStorage, FileStorage) without changing the engine. `TimerProvider` enables time-mocked testing |
| **Generics with Trait Bounds** | `Engine[S]` and `fn[S : Storage] Engine::new` demonstrate MoonBit's type-parameter system |
| **Result/Option** | All fallible operations return `Result[T, WorkflowError]` — no panics, no unchecked exceptions |
| **Struct Mutation** | `mut` fields on `WorkflowExecutionState`, `CircuitBreaker`, `WorkflowMetrics` enable efficient in-place state updates with explicit mutation tracking |
| **Pattern Matching** | Engine dispatch and replay use exhaustive pattern matching on ADTs for type-safe control flow |
| **Immutable Data Structures** | `EventLog::append()` returns a new EventLog — the original is never mutated, ensuring concurrent readers see consistent snapshots |

## License

Apache-2.0

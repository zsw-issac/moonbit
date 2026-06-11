///|
/// MoonFlow — Deterministic Workflow Engine for MoonBit
///
/// MoonFlow provides a deterministic workflow execution engine built on event sourcing
/// and replay-based recovery. Workflows survive process crashes and can be replayed
/// for debugging and auditing.
///
/// ## Quick Start
///
/// ```moonbit
/// let engine = new_memory_engine()
/// engine.register("hello", fn(input : Json) -> Async[Result[Json, WorkflowError]] {
///   Async::pure(Ok(input))
/// })
/// let wf = workflow("example")
///   .then("hello", StepConfig::default("hello"))
///   .build()
/// let result = Async::run(engine.start(wf, Json::null()))
/// ```
///
/// ## Architecture
///
/// MoonFlow uses **Event Sourcing**: every state transition is recorded as an immutable
/// `WorkflowEvent`. The execution state is reconstructed from events via `replay_events()`,
/// a pure, deterministic function.
///
/// ## MoonBit Feature Usage
///
/// MoonFlow leverages key MoonBit language features:
///
/// - **ADTs (Algebraic Data Types)**: `WorkflowEvent`, `WorkflowError`, `WorkflowStatus`,
///   and `StepType` use MoonBit's `enum` types to represent state machines cleanly
///   without nulls or exceptions.
///
/// - **Traits for Extensibility**: The `Storage` trait enables swapping backends
///   (MemoryStorage, FileStorage, database) without changing the engine. `TimerProvider`
///   enables time-mocked testing.
///
/// - **Generics with Trait Bounds**: `Engine[S]` and functions like `fn[S : Storage] Engine::new`
///   demonstrate MoonBit's type-parameter + trait-bound system.
///
/// - **Result/Option for Safe Error Handling**: All fallible operations return
///   `Result[T, WorkflowError]` — no panics, no unchecked exceptions.
///
/// - **Struct Mutation**: `mut` fields on `WorkflowExecutionState` enable efficient
///   in-place state updates while maintaining explicit mutation tracking.
///
/// - **Pattern Matching**: The engine's dispatch logic and replay function use
///   exhaustive pattern matching on ADTs for type-safe control flow.

///|
/// Create a new workflow engine backed by in-memory storage.
/// Suitable for testing and single-node development.
///
/// # Example
/// ```
/// let engine = new_memory_engine()
/// ```
pub fn new_memory_engine() -> Engine[MemoryStorage] {
  Engine::new(MemoryStorage::new())
}

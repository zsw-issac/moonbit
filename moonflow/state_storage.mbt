///|
/// Storage trait: abstract backend for event persistence.
///
/// All methods return `Async[Result[...]]` to support async backends
/// (file I/O, network databases) while maintaining compatibility with
/// synchronous backends via `Async::pure()` wrapping.
///
/// ## Implementations
/// - `MemoryStorage`: synchronous, results wrapped in `Async::pure()`
/// - `FileStorage`: synchronous stub, results wrapped in `Async::pure()`
pub trait Storage {
  /// Save an event to the workflow's event log.
  save_event(Self, WorkflowId, WorkflowEvent) -> Async[Result[Unit, WorkflowError]]

  /// Load the complete event log for a workflow.
  load_events(Self, WorkflowId) -> Async[Result[Array[WorkflowEvent], WorkflowError]]

  /// Save a snapshot for incremental recovery optimization.
  save_snapshot(Self, WorkflowId, WorkflowSnapshot) -> Async[Result[Unit, WorkflowError]]

  /// Load the latest snapshot for a workflow.
  load_snapshot(Self, WorkflowId) -> Async[Result[WorkflowSnapshot?, WorkflowError]]

  /// List all tracked workflow instance IDs.
  list_workflows(Self) -> Async[Result[Array[WorkflowId], WorkflowError]]
}

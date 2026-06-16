///|
/// Workflow-level timeout enforcement.
///
/// Wraps Engine::start with a global timeout. If the workflow exceeds
/// the timeout, a WorkflowFailed event is recorded.

/// Start a workflow with a global timeout. Returns timeout error if exceeded.
pub fn[S : Storage] Engine::start_with_timeout(
  self : Engine[S],
  wf_def : WorkflowDef,
  input : Json,
  timeout_ms : Int
) -> Async[Result[WorkflowId, WorkflowError]] {
  let start = @env.now().to_int64()
  let result = self.start(wf_def, input).run()
  let elapsed = @env.now().to_int64() - start
  match result {
    Ok(wf_id) => {
      if timeout_ms > 0 && elapsed > Int64::from_int(timeout_ms) {
        // Timeout exceeded — record failure
        self.store_event(wf_id, WorkflowEvent::WorkflowFailed(wf_id, "workflow timeout exceeded", @env.now().to_int64()))
        Async::pure(Err(WorkflowError::StepTimeout("workflow", timeout_ms)))
      } else {
        Async::pure(Ok(wf_id))
      }
    }
    Err(err) => Async::pure(Err(err))
  }
}

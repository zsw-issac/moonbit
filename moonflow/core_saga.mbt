///|
/// Saga — distributed transaction orchestration with automatic compensation.
///
/// A Saga is a sequence of steps where each step has a `forward` action and a
/// `compensate` action. If any forward step fails, all previously completed
/// steps are compensated in **reverse order**, ensuring eventual consistency
/// without distributed locks.
///
/// ## Pattern
///
/// ```
/// Step 1: forward → ✅
/// Step 2: forward → ✅
/// Step 3: forward → ❌ (fails!)
///          → compensate Step 2
///          → compensate Step 1
///          → Saga failed (fully rolled back)
/// ```
///
/// ## Example
///
/// ```moonbit
/// // Transfer money: debit A → credit B
/// // Compensation: credit A ← debit B
/// let saga = Saga::new(engine, "transfer")
///   .step("debit_A", debit_handler, compensate_debit_handler)
///   .step("credit_B", credit_handler, compensate_credit_handler)
///   .execute(input)
/// ```

///|
/// SagaStep: a single step in a saga, with both forward and compensating actions.
pub struct SagaStep {
  name : String
  forward : StepExecutor
  compensate : StepExecutor
}

///|
/// Saga orchestrator: coordinates forward execution and automatic rollback.
pub struct Saga[S] {
  engine : Engine[S]
  storage : S
  steps : Array[SagaStep]
  saga_name : String
}

///|
/// Create a new Saga orchestrator.
pub fn[S : Storage] Saga::new(engine : Engine[S], storage : S, name : String) -> Saga[S] {
  { engine, storage, steps: [], saga_name: name }
}

///|
/// Add a step to the saga with its forward and compensating actions.
pub fn[S : Storage] Saga::step(
  self : Saga[S],
  name : String,
  forward : StepExecutor,
  compensate : StepExecutor
) -> Saga[S] {
  self.steps.push({ name, forward, compensate })
  self
}

///|
/// Execute the saga: runs forward steps sequentially. On failure, runs
/// compensations in reverse order for all previously completed steps.
///
/// Returns the output of the last successful forward step on success,
/// or the error that caused the saga to fail on failure (after compensation).
pub fn[S : Storage] Saga::execute(
  self : Saga[S],
  input : Json
) -> Async[Result[Json, WorkflowError]] {
  let mut current_input = input
  let completed_indices : Array[Int] = Array::new()

  // Forward phase: execute steps in order
  for i = 0; i < self.steps.length(); i = i + 1 {
    let step = self.steps[i]
    let handler : StepExecutor = step.forward
    let result = handler(current_input).run()

    match result {
      Ok(output) => {
        current_input = output
        completed_indices.push(i)
      }
      Err(err) => {
        // Failure! Begin compensation in reverse order
        let comp_error = self.compensate(completed_indices, current_input)
        match comp_error {
          Some(comp_err) => {
            return Async::pure(Err(WorkflowError::StorageError(
              "Saga failed and compensation also failed: \{comp_err}"
            )))
          }
          None => ignore(())
        }
        return Async::pure(Err(err))
      }
    }
  }

  Async::pure(Ok(current_input))
}

///|
/// Run compensations in reverse order for completed steps.
/// Returns Some(error_message) if any compensation fails, None if all succeed.
fn[S : Storage] Saga::compensate(
  self : Saga[S],
  completed_indices : Array[Int],
  input : Json
) -> String? {
  // Reverse order: last completed first
  let mut current_input = input
  let mut i = completed_indices.length()

  while i > 0 {
    i = i - 1
    let idx = completed_indices[i]
    let step = self.steps[idx]
    let handler : StepExecutor = step.compensate
    let result = handler(current_input).run()
    match result {
      Ok(output) => current_input = output
      Err(err) => return Some("compensation for '\{step.name}' failed: \{saga_err_str(err)}")
    }
  }

  None
}

///|
/// Create a saga step from already-registered engine handlers.
/// The forward handler is looked up from the engine registry.
pub fn[S : Storage] Saga::step_by_name(
  self : Saga[S],
  name : String,
  compensate : StepExecutor
) -> Saga[S] {
  let forward = match self.engine.registry.get(name) {
    Some(h) => h
    None => {
      // Use a failing handler as fallback
      fn(_input : Json) -> Async[Result[Json, WorkflowError]] {
        Async::pure(Err(WorkflowError::StepFailed(name, "handler not found")))
      }
    }
  }
  self.steps.push({ name, forward, compensate })
  self
}

fn saga_err_str(err : WorkflowError) -> String {
  match err {
    WorkflowError::StepFailed(n, r) => "StepFailed(\{n}, \{r})"
    WorkflowError::StepTimeout(n, t) => "StepTimeout(\{n}, \{t})"
    WorkflowError::StepMaxRetriesExceeded(n, a) => "MaxRetries(\{n}, \{a})"
    WorkflowError::WorkflowNotFound(id) => "NotFound(\{id.value})"
    WorkflowError::InvalidTransition(f, t) => "InvalidTransition(\{f}, \{t})"
    WorkflowError::SerializationError(m) => "Serialization(\{m})"
    WorkflowError::StorageError(m) => "Storage(\{m})"
  }
}

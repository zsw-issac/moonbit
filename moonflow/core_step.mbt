///|
/// StepResult: result of a single step execution.
pub enum StepResult[T] {
  Success(T)
  Failure(WorkflowError)
  Pending
}

///|
/// StepConfig: configuration for a workflow step.
pub struct StepConfig {
  name : String
  max_retry : Int
  timeout_ms : Int
  retry_delay_ms : Int
}

pub fn StepConfig::default(name : String) -> StepConfig {
  { name, max_retry: 3, timeout_ms: 30000, retry_delay_ms: 1000 }
}

pub fn StepConfig::with_retry(name : String, max_retry : Int) -> StepConfig {
  { name, max_retry, timeout_ms: 30000, retry_delay_ms: 1000 }
}

pub fn StepConfig::with_timeout(name : String, timeout_ms : Int) -> StepConfig {
  { name, max_retry: 3, timeout_ms, retry_delay_ms: 1000 }
}

pub fn StepConfig::with_all(name : String, max_retry : Int, timeout_ms : Int, retry_delay_ms : Int) -> StepConfig {
  { name, max_retry, timeout_ms, retry_delay_ms }
}

///|
/// StepNode: a node in the workflow DAG.
pub struct StepNode {
  name : String
  config : StepConfig
  step_type : StepType
}

///|
/// StepType: determines how a step is executed.
pub enum StepType {
  Sequential
  Parallel(Array[String])
  Conditional(String, String, String)
}

///|
/// Step executor type: takes Json input, returns Async[Result[Json, WorkflowError]].
pub type StepExecutor = (Json) -> Async[Result[Json, WorkflowError]]

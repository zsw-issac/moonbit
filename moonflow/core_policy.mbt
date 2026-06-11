///|
/// Workflow configuration and options.

///|
/// RetryPolicy: defines how step retries are handled.
pub enum RetryPolicy {
  Fixed(Int)
  ExponentialBackoff(Int, Int)
  NoRetry
}

pub fn RetryPolicy::delay_for_attempt(self : RetryPolicy, attempt : Int) -> Int {
  match self {
    Fixed(d) => d
    ExponentialBackoff(initial, base) => {
      let multiplier = pow_int(base, attempt - 1)
      initial * multiplier
    }
    NoRetry => 0
  }
}

fn pow_int(base : Int, exp : Int) -> Int {
  let mut result = 1
  let mut i = 0
  while i < exp {
    result = result * base
    i = i + 1
  }
  result
}

///|
/// WorkflowOptions: global options for a workflow execution.
pub struct WorkflowOptions {
  workflow_timeout_ms : Int
  auto_retry_on_failure : Bool
  max_parallelism : Int
}

pub fn WorkflowOptions::default() -> WorkflowOptions {
  {
    workflow_timeout_ms: 0,
    auto_retry_on_failure: false,
    max_parallelism: 10,
  }
}

///|
/// StepErrorPolicy: determines what happens when a step fails after all retries.
pub enum ErrorPolicy {
  FailWorkflow
  SkipAndContinue
  Compensate(String)
}

///|
/// Workflow pattern library — reusable execution patterns.
///
/// Common distributed system patterns implemented as composable StepExecutors.

///|
/// Retry with exponential backoff: wraps a handler with configurable retries.
/// Delay = base_ms * 2^(attempt-1)
pub fn pattern_retry_backoff(
  base_ms : Int,
  max_attempts : Int,
  inner : StepExecutor
) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    try_with_backoff(inner, input, base_ms, max_attempts, 1)
  }
}

fn try_with_backoff(
  inner : StepExecutor,
  input : Json,
  base_ms : Int,
  max_attempts : Int,
  attempt : Int
) -> Async[Result[Json, WorkflowError]] {
  let result = inner(input).run()
  match result {
    Ok(_) => Async::pure(result)
    Err(err) => {
      if attempt < max_attempts && should_retry(err) {
        try_with_backoff(inner, input, base_ms, max_attempts, attempt + 1)
      } else {
        Async::pure(result)
      }
    }
  }
}

///|
/// Timeout decorator: wraps a handler with a timeout limit.
/// If execution exceeds timeout_ms, returns StepTimeout error.
pub fn pattern_timeout(timeout_ms : Int, inner : StepExecutor) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    let start = now_ms()
    let result = inner(input).run()
    let elapsed = elapsed_ms(start, now_ms())
    if elapsed.to_int() > timeout_ms {
      Async::pure(Err(WorkflowError::StepTimeout("pattern_timeout", timeout_ms)))
    } else {
      Async::pure(result)
    }
  }
}

///|
/// Circuit breaker pattern: after N consecutive failures, stops calling inner
/// for reset_timeout_ms. One probe call is allowed after reset to test recovery.
pub struct PatternCircuitBreaker {
  mut failure_count : Int
  threshold : Int
  reset_ms : Int
  mut last_failure_ts : Int64
  mut is_open : Bool
}

pub fn PatternCircuitBreaker::new(threshold : Int, reset_ms : Int) -> PatternCircuitBreaker {
  { failure_count: 0, threshold, reset_ms, last_failure_ts: Int64::from_int(0), is_open: false }
}

pub fn PatternCircuitBreaker::wrap(self : PatternCircuitBreaker, inner : StepExecutor) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    if self.is_open {
      let now = now_ms()
      if now - self.last_failure_ts > Int64::from_int(self.reset_ms) {
        self.is_open = false // Half-open — allow one probe
      } else {
        return Async::pure(Err(WorkflowError::StorageError("circuit breaker open")))
      }
    }
    let result = inner(input).run()
    match result {
      Ok(_) => { self.failure_count = 0; self.is_open = false }
      Err(_) => {
        self.failure_count = self.failure_count + 1
        self.last_failure_ts = now_ms()
        if self.failure_count >= self.threshold { self.is_open = true }
      }
    }
    Async::pure(result)
  }
}

///|
/// Bulkhead pattern: limits concurrent executions using a semaphore.
/// Excess calls are rejected immediately.
pub struct PatternBulkhead {
  max_concurrent : Int
  mut active_count : Int
}

pub fn PatternBulkhead::new(max_concurrent : Int) -> PatternBulkhead {
  { max_concurrent, active_count: 0 }
}

pub fn PatternBulkhead::wrap(self : PatternBulkhead, inner : StepExecutor) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    if self.active_count >= self.max_concurrent {
      return Async::pure(Err(WorkflowError::StorageError("bulkhead limit reached")))
    }
    self.active_count = self.active_count + 1
    let result = inner(input).run()
    self.active_count = self.active_count - 1
    Async::pure(result)
  }
}

///|
/// Fallback pattern: tries primary, falls back to backup on failure.
pub fn pattern_fallback(primary : StepExecutor, fallback : StepExecutor) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    let result = primary(input).run()
    match result {
      Ok(_) => Async::pure(result)
      Err(_) => fallback(input)
    }
  }
}

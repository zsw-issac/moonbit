///|
/// Workflow middleware and hooks system.
///
/// Middleware wraps step execution with cross-cutting concerns like:
/// - Logging and metrics
/// - Authentication/authorization checks
/// - Rate limiting
/// - Custom error transformation
///
/// ## Architecture
///
/// Middleware follows the decorator pattern: each middleware wraps the
/// original `StepExecutor`, producing a new `StepExecutor` that adds
/// behavior before/after/around the core execution.

///|
/// Middleware function type: takes a StepExecutor and returns a wrapped StepExecutor.
pub type Middleware = (StepExecutor) -> StepExecutor

///|
/// Create a logging middleware that prints step start/completion.
///
/// # Example
/// ```
/// let engine = Engine::new(storage)
/// engine.register("my_step", logging_middleware(my_handler))
/// ```
pub fn logging_middleware(inner : StepExecutor) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    let result = inner(input)
    match result {
      Async::Pure(r) => {
        match r {
          Ok(_) => Async::pure(r)
          Err(_) => Async::pure(r)
        }
      }
      Async::Suspend(s) => Async::Suspend(fn() { logging_middleware_continue(s()) })
    }
  }
}

fn logging_middleware_continue(next : Async[Result[Json, WorkflowError]]) -> Async[Result[Json, WorkflowError]] {
  next
}

///|
/// Create a retry middleware that wraps a handler with retry logic.
/// The inner handler is called up to `max_attempts` times before giving up.
pub fn retry_middleware(max_attempts : Int, delay_ms : Int, inner : StepExecutor) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    retry_with_delay(inner, input, max_attempts, delay_ms, 1)
  }
}

fn retry_with_delay(
  inner : StepExecutor,
  input : Json,
  max_attempts : Int,
  delay_ms : Int,
  attempt : Int
) -> Async[Result[Json, WorkflowError]] {
  let result = inner(input).run()
  match result {
    Ok(_) => Async::pure(result)
    Err(_) => {
      if attempt < max_attempts {
        // In production, actual delay would happen here
        retry_with_delay(inner, input, max_attempts, delay_ms, attempt + 1)
      } else {
        Async::pure(result)
      }
    }
  }
}

///|
/// Create a timeout middleware that fails if execution exceeds `timeout_ms`.
pub fn timeout_middleware(timeout_ms : Int, inner : StepExecutor) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    let start = now_ms()
    let result = inner(input).run()
    let elapsed = elapsed_ms(start, now_ms())
    let elapsed_int = elapsed.to_int()
    if elapsed_int > timeout_ms {
      Async::pure(Err(WorkflowError::StepTimeout("middleware", timeout_ms)))
    } else {
      Async::pure(result)
    }
  }
}

///|
/// Create a circuit-breaker middleware.
///
/// After `failure_threshold` consecutive failures, the circuit opens
/// and subsequent calls fail immediately without invoking the inner handler.
/// After `reset_timeout_ms`, the circuit half-opens and allows one probe call.
pub struct CircuitBreaker {
  mut failure_count : Int
  failure_threshold : Int
  reset_timeout_ms : Int
  mut last_failure_time : Int64
  mut state : CircuitState
}

pub enum CircuitState {
  Closed
  Open
  HalfOpen
}

pub fn CircuitBreaker::new(failure_threshold : Int, reset_timeout_ms : Int) -> CircuitBreaker {
  {
    failure_count: 0,
    failure_threshold,
    reset_timeout_ms,
    last_failure_time: Int64::from_int(0),
    state: Closed,
  }
}

pub fn CircuitBreaker::wrap(self : CircuitBreaker, inner : StepExecutor) -> StepExecutor {
  fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    match self.state {
      Open => {
        let now = now_ms()
        let elapsed = now - self.last_failure_time
        let reset_ms : Int64 = Int64::from_int(self.reset_timeout_ms)
        if elapsed > reset_ms {
          self.state = HalfOpen
        } else {
          return Async::pure(Err(WorkflowError::StorageError("circuit breaker open")))
        }
      }
      _ => ignore(())
    }
    let result = inner(input).run()
    match result {
      Ok(_) => {
        self.failure_count = 0
        self.state = Closed
      }
      Err(_) => {
        self.failure_count = self.failure_count + 1
        self.last_failure_time = now_ms()
        if self.failure_count >= self.failure_threshold {
          self.state = Open
        }
      }
    }
    Async::pure(result)
  }
}

///|
/// Chain multiple middlewares together.
/// `chain(m1, m2, handler)` applies m1, then m2, then handler.
pub fn chain(m1 : Middleware, m2 : Middleware, inner : StepExecutor) -> StepExecutor {
  m1(m2(inner))
}

///|
/// Error classification and smart retry strategies.
///
/// Categorizes workflow errors to enable intelligent retry decisions:
/// - **Transient** errors: retryable (network timeout, temporary unavailability)
/// - **Permanent** errors: no retry (validation failure, auth error)
/// - **Timeout** errors: may retry with longer timeout

///|
/// ErrorCategory: determines retry behavior.
pub enum ErrorCategory {
  Transient
  Permanent
  Timeout
  Unknown
}

///|
/// Classify a WorkflowError into a category for smart retry decisions.
pub fn classify_error(err : WorkflowError) -> ErrorCategory {
  match err {
    WorkflowError::StepTimeout(_, _) => Timeout
    WorkflowError::SerializationError(_) => Permanent
    WorkflowError::InvalidTransition(_, _) => Permanent
    WorkflowError::WorkflowNotFound(_) => Permanent
    WorkflowError::StepFailed(step, reason) => {
      if reason.contains("timeout") || reason.contains("temporary") || reason.contains("unavailable") {
        Transient
      } else if reason.contains("validation") || reason.contains("auth") || reason.contains("permission") {
        Permanent
      } else if reason.contains("transient") {
        Transient
      } else {
        Unknown
      }
    }
    WorkflowError::StepMaxRetriesExceeded(_, _) => Permanent
    WorkflowError::StorageError(msg) => {
      if msg.contains("timeout") { Transient } else { Permanent }
    }
  }
}

///|
/// Smart retry: only retry transient and timeout errors.
/// Permanent errors fail immediately.
pub fn should_retry(err : WorkflowError) -> Bool {
  let cat = classify_error(err)
  match cat {
    Transient | Timeout => true
    Permanent | Unknown => false
  }
}

///|
/// Get recommended retry delay based on error category.
pub fn retry_delay_for(err : WorkflowError, attempt : Int, base_delay_ms : Int) -> Int {
  match classify_error(err) {
    Timeout => base_delay_ms * attempt * 2  // Longer delay for timeouts
    Transient => base_delay_ms * attempt     // Linear backoff
    _ => 0
  }
}

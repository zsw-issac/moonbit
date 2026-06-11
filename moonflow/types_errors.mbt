///|
/// WorkflowError: all error types used in the workflow engine.
pub enum WorkflowError {
  StepFailed(String, String)
  StepTimeout(String, Int)
  StepMaxRetriesExceeded(String, Int)
  WorkflowNotFound(WorkflowId)
  InvalidTransition(String, String)
  SerializationError(String)
  StorageError(String)
} derive(Show, Eq)

pub fn WorkflowError::step_failed(name : String, reason : String) -> WorkflowError {
  StepFailed(name, reason)
}

pub fn WorkflowError::step_timeout(name : String, timeout_ms : Int) -> WorkflowError {
  StepTimeout(name, timeout_ms)
}

pub fn WorkflowError::max_retries(name : String, attempts : Int) -> WorkflowError {
  StepMaxRetriesExceeded(name, attempts)
}

pub fn WorkflowError::not_found(id : WorkflowId) -> WorkflowError {
  WorkflowNotFound(id)
}

pub fn WorkflowError::invalid_transition(from : String, to : String) -> WorkflowError {
  InvalidTransition(from, to)
}

pub fn WorkflowError::serialization(msg : String) -> WorkflowError {
  SerializationError(msg)
}

pub fn WorkflowError::storage(msg : String) -> WorkflowError {
  StorageError(msg)
}

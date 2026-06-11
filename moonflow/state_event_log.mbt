///|
/// WorkflowEvent: all state transitions recorded as events (event sourcing core).
///
/// Each event captures a specific state transition in the workflow lifecycle.
/// Events are stored in an immutable append-only log and serve as the single
/// source of truth for replay and crash recovery.
pub enum WorkflowEvent {
  WorkflowStarted(WorkflowId, String, Json, Int64)
  StepStarted(WorkflowId, String, Int, Int64)
  StepCompleted(WorkflowId, String, Json, Int64)
  StepFailed(WorkflowId, String, String, Int, Int64)
  StepRetrying(WorkflowId, String, Int, Int, Int64)
  WorkflowCompleted(WorkflowId, Json, Int64)
  WorkflowFailed(WorkflowId, String, Int64)
}

pub fn WorkflowEvent::workflow_started(id : WorkflowId, name : String, input : Json, ts : Int64) -> WorkflowEvent {
  WorkflowStarted(id, name, input, ts)
}

pub fn WorkflowEvent::step_started(id : WorkflowId, name : String, attempt : Int, ts : Int64) -> WorkflowEvent {
  StepStarted(id, name, attempt, ts)
}

pub fn WorkflowEvent::step_completed(id : WorkflowId, name : String, output : Json, ts : Int64) -> WorkflowEvent {
  StepCompleted(id, name, output, ts)
}

pub fn WorkflowEvent::step_failed(id : WorkflowId, name : String, err : String, attempt : Int, ts : Int64) -> WorkflowEvent {
  StepFailed(id, name, err, attempt, ts)
}

pub fn WorkflowEvent::step_retrying(id : WorkflowId, name : String, attempt : Int, delay : Int, ts : Int64) -> WorkflowEvent {
  StepRetrying(id, name, attempt, delay, ts)
}

pub fn WorkflowEvent::workflow_completed(id : WorkflowId, output : Json, ts : Int64) -> WorkflowEvent {
  WorkflowCompleted(id, output, ts)
}

pub fn WorkflowEvent::workflow_failed(id : WorkflowId, err : String, ts : Int64) -> WorkflowEvent {
  WorkflowFailed(id, err, ts)
}

///|
/// EventLog: immutable append-only log of events for a workflow instance.
///
/// **Immutability Guarantee**: `append()` returns a **new** EventLog with the
/// event appended. The original EventLog is never modified. This ensures that
/// concurrent readers see a consistent snapshot of the event history.
pub struct EventLog {
  workflow_id : WorkflowId
  events : Array[WorkflowEvent]
}

///|
/// Create a new empty EventLog for the given workflow.
pub fn EventLog::new(id : WorkflowId) -> EventLog {
  { workflow_id: id, events: [] }
}

///|
/// Append an event to the log, returning a **new** EventLog.
///
/// The original EventLog is untouched. This is an O(n) operation (array copy)
/// but ensures immutability — callers holding a reference to the old EventLog
/// still see the original event list.
pub fn EventLog::append(self : EventLog, event : WorkflowEvent) -> EventLog {
  let new_events = Array::new()
  for e in self.events {
    new_events.push(e)
  }
  new_events.push(event)
  { workflow_id: self.workflow_id, events: new_events }
}

///|
/// Get the number of events in this log.
pub fn EventLog::length(self : EventLog) -> Int {
  self.events.length()
}

///|
/// Get an event by index. Returns None if out of range.
pub fn EventLog::get(self : EventLog, index : Int) -> WorkflowEvent? {
  if index < 0 || index >= self.events.length() {
    return None
  }
  Some(self.events[index])
}

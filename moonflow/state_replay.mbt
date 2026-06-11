///|
/// WorkflowExecutionState: reconstructed state from replaying events.
pub struct WorkflowExecutionState {
  workflow_id : WorkflowId
  mut status : WorkflowStatus
  mut completed_steps : Map[String, Json]
  mut failed_steps : Map[String, WorkflowError]
  mut current_step : String
  mut step_attempts : Map[String, Int]
}

pub fn WorkflowExecutionState::new(id : WorkflowId) -> WorkflowExecutionState {
  {
    workflow_id: id,
    status: Pending,
    completed_steps: Map::new(),
    failed_steps: Map::new(),
    current_step: "",
    step_attempts: Map::new(),
  }
}

///|
/// replay_events: pure function that rebuilds execution state from event log.
/// Deterministic: same events always produce the same state.
pub fn replay_events(events : Array[WorkflowEvent]) -> Result[WorkflowExecutionState, WorkflowError] {
  let mut state : WorkflowExecutionState? = None
  for event in events {
    state = Some(apply_event(state, event))
  }
  match state {
    Some(s) => Ok(s)
    None => Err(WorkflowError::SerializationError("empty event log"))
  }
}

fn apply_event(
  state : WorkflowExecutionState?,
  event : WorkflowEvent
) -> WorkflowExecutionState {
  match (state, event) {
    (None, WorkflowEvent::WorkflowStarted(_, _, _, _)) => {
      let s = WorkflowExecutionState::new(WorkflowId::new(""))
      s.status = Running
      s
    }
    (Some(s), WorkflowEvent::StepStarted(_, step, attempt, _)) => {
      s.current_step = step
      s.step_attempts[step] = attempt
      s
    }
    (Some(s), WorkflowEvent::StepCompleted(_, step, output, _)) => {
      s.completed_steps[step] = output
      if s.failed_steps.contains(step) { s.failed_steps.remove(step) }
      s
    }
    (Some(s), WorkflowEvent::StepFailed(_, step, err, _, _)) => {
      s.failed_steps[step] = WorkflowError::StepFailed(step, err)
      s
    }
    (Some(s), WorkflowEvent::StepRetrying(_, step, attempt, _, _)) => {
      s.step_attempts[step] = attempt
      s
    }
    (Some(s), WorkflowEvent::WorkflowCompleted(_, _, _)) => {
      s.status = Completed
      s
    }
    (Some(s), WorkflowEvent::WorkflowFailed(_, _, _)) => {
      s.status = Failed
      s
    }
    _ => {
      match state {
        Some(s) => s
        None => WorkflowExecutionState::new(WorkflowId::new(""))
      }
    }
  }
}

///|
/// Workflow result retrieval API.
pub struct WorkflowResult {
  workflow_id : WorkflowId
  output : Json
  events_count : Int
  steps_executed : Int
  elapsed_ms : Int64
}

fn load_r[S : Storage](s : S, id : WorkflowId) -> Array[WorkflowEvent] {
  match s.load_events(id).run() { Ok(e) => e; Err(_) => [] }
}

pub fn[S : Storage] get_result(storage : S, id : WorkflowId) -> WorkflowResult? {
  let events = load_r(storage, id)
  if events.length() == 0 { return None }
  match replay_events(events) {
    Ok(state) => {
      if state.status != Completed { return None }
      let output = match events[events.length() - 1] { WorkflowEvent::WorkflowCompleted(_, out, _) => out; _ => Json::null() }
      let started_ts = match events[0] { WorkflowEvent::WorkflowStarted(_, _, _, ts) => ts; _ => Int64::from_int(0) }
      let completed_ts = match events[events.length() - 1] { WorkflowEvent::WorkflowCompleted(_, _, ts) => ts; _ => Int64::from_int(0) }
      Some({ workflow_id: id, output, events_count: events.length(), steps_executed: state.completed_steps.length(), elapsed_ms: completed_ts - started_ts })
    }
    Err(_) => None
  }
}

pub fn[S : Storage] get_error(storage : S, id : WorkflowId) -> (String, String)? {
  let events = load_r(storage, id)
  if events.length() == 0 { return None }
  match replay_events(events) {
    Ok(state) => {
      if state.status != Failed { return None }
      match events[events.length() - 1] { WorkflowEvent::WorkflowFailed(_, msg, _) => Some(("WorkflowFailed", msg)); _ => None }
    }
    Err(_) => None
  }
}

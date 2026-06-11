///|
/// Workflow query and monitoring API.
pub struct WorkflowSummary {
  id : WorkflowId
  name : String
  status : WorkflowStatus
  step_count : Int
  completed_count : Int
  created_at : Int64
}

fn load_events_sync[S : Storage](storage : S, id : WorkflowId) -> Array[WorkflowEvent] {
  match storage.load_events(id).run() {
    Ok(events) => events
    Err(_) => []
  }
}

pub fn[S : Storage] query_status(storage : S, id : WorkflowId) -> WorkflowStatus? {
  let events = load_events_sync(storage, id)
  if events.length() == 0 { return None }
  match replay_events(events) {
    Ok(s) => Some(s.status)
    Err(_) => None
  }
}

pub fn[S : Storage] query_summary(storage : S, id : WorkflowId) -> WorkflowSummary? {
  let events = load_events_sync(storage, id)
  if events.length() == 0 { return None }
  let state_r = replay_events(events)
  match state_r {
    Ok(state) => {
      let name = match events[0] { WorkflowEvent::WorkflowStarted(_, n, _, _) => n; _ => "unknown" }
      let created = match events[0] { WorkflowEvent::WorkflowStarted(_, _, _, ts) => ts; _ => Int64::from_int(0) }
      Some({ id, name, status: state.status, step_count: count_total_steps(events), completed_count: state.completed_steps.length(), created_at: created })
    }
    Err(_) => None
  }
}

pub fn[S : Storage] list_all_workflows(storage : S) -> Array[WorkflowId] {
  match storage.list_workflows().run() { Ok(ids) => ids; Err(_) => [] }
}

pub fn[S : Storage] count_by_status(storage : S) -> Map[String, Int] {
  let result : Map[String, Int] = Map::new()
  let wf_ids = list_all_workflows(storage)
  for wid in wf_ids {
    match query_status(storage, wid) {
      Some(status) => {
        let key = status_label(status)
        let count = match result.get(key) { Some(c) => c + 1; None => 1 }
        result[key] = count
      }
      None => ignore(wid)
    }
  }
  result
}

fn count_total_steps(events : Array[WorkflowEvent]) -> Int {
  let steps : Map[String, Unit] = Map::new()
  for event in events {
    match event {
      WorkflowEvent::StepStarted(_, name, _, _) => steps[name] = ()
      WorkflowEvent::StepCompleted(_, name, _, _) => steps[name] = ()
      _ => ignore(event)
    }
  }
  steps.length()
}

fn status_label(s : WorkflowStatus) -> String {
  match s { Pending => "Pending"; Running => "Running"; Completed => "Completed"; Failed => "Failed"; Cancelled => "Cancelled" }
}

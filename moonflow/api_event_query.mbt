///|
/// Event query API — filter, count, and analyze workflow events.
///
/// Provides utilities for introspecting event logs:
/// - Filter events by type, step name, or time range
/// - Count events by category (success/failure/retry)
/// - Compute execution statistics

fn load_ev[S : Storage](storage : S, id : WorkflowId) -> Array[WorkflowEvent] {
  match storage.load_events(id).run() { Ok(events) => events; Err(_) => [] }
}

/// Filter events by type. Returns only events matching any of the given types.
pub fn[S : Storage] filter_by_type(
  storage : S, id : WorkflowId, types : Array[String]
) -> Array[WorkflowEvent] {
  let events = load_events_sync(storage, id)
  let result : Array[WorkflowEvent] = Array::new()
  for e in events {
    let type_name = event_type_name(e)
    if array_contains(types, type_name) {
      result.push(e)
    }
  }
  result
}

/// Filter events by step name. Returns only events for the given step.
pub fn[S : Storage] filter_by_step(
  storage : S, id : WorkflowId, step_name : String
) -> Array[WorkflowEvent] {
  let events = load_events_sync(storage, id)
  let result : Array[WorkflowEvent] = Array::new()
  for e in events {
    let e_step = event_step_name(e)
    match e_step {
      Some(n) => if n == step_name { result.push(e) }
      None => ignore(e)
    }
  }
  result
}

/// Filter events by time range (inclusive).
pub fn[S : Storage] filter_by_time(
  storage : S, id : WorkflowId, start_ms : Int64, end_ms : Int64
) -> Array[WorkflowEvent] {
  let events = load_events_sync(storage, id)
  let result : Array[WorkflowEvent] = Array::new()
  for e in events {
    let ts = event_timestamp(e)
    match ts {
      Some(t) => if t >= start_ms && t <= end_ms { result.push(e) }
      None => ignore(e)
    }
  }
  result
}

/// EventStats: aggregated counts by category.
pub struct EventStats {
  total_events : Int
  started_count : Int
  completed_count : Int
  failed_count : Int
  retrying_count : Int
  workflow_started : Int
  workflow_completed : Int
  workflow_failed : Int
}

/// Compute event statistics for a workflow.
pub fn[S : Storage] compute_stats(storage : S, id : WorkflowId) -> EventStats {
  let events = load_events_sync(storage, id)
  let mut stats = EventStats {
    total_events: events.length(),
    started_count: 0, completed_count: 0, failed_count: 0, retrying_count: 0,
    workflow_started: 0, workflow_completed: 0, workflow_failed: 0,
  }
  for e in events {
    match e {
      WorkflowEvent::WorkflowStarted(_, _, _, _) => stats.workflow_started = stats.workflow_started + 1
      WorkflowEvent::StepStarted(_, _, _, _) => stats.started_count = stats.started_count + 1
      WorkflowEvent::StepCompleted(_, _, _, _) => stats.completed_count = stats.completed_count + 1
      WorkflowEvent::StepFailed(_, _, _, _, _) => stats.failed_count = stats.failed_count + 1
      WorkflowEvent::StepRetrying(_, _, _, _, _) => stats.retrying_count = stats.retrying_count + 1
      WorkflowEvent::WorkflowCompleted(_, _, _) => stats.workflow_completed = stats.workflow_completed + 1
      WorkflowEvent::WorkflowFailed(_, _, _) => stats.workflow_failed = stats.workflow_failed + 1
    }
  }
  stats
}

fn event_type_name(e : WorkflowEvent) -> String {
  match e {
    WorkflowEvent::WorkflowStarted(_, _, _, _) => "WorkflowStarted"
    WorkflowEvent::StepStarted(_, _, _, _) => "StepStarted"
    WorkflowEvent::StepCompleted(_, _, _, _) => "StepCompleted"
    WorkflowEvent::StepFailed(_, _, _, _, _) => "StepFailed"
    WorkflowEvent::StepRetrying(_, _, _, _, _) => "StepRetrying"
    WorkflowEvent::WorkflowCompleted(_, _, _) => "WorkflowCompleted"
    WorkflowEvent::WorkflowFailed(_, _, _) => "WorkflowFailed"
  }
}

fn event_step_name(e : WorkflowEvent) -> String? {
  match e {
    WorkflowEvent::StepStarted(_, name, _, _) => Some(name)
    WorkflowEvent::StepCompleted(_, name, _, _) => Some(name)
    WorkflowEvent::StepFailed(_, name, _, _, _) => Some(name)
    WorkflowEvent::StepRetrying(_, name, _, _, _) => Some(name)
    _ => None
  }
}

fn event_timestamp(e : WorkflowEvent) -> Int64? {
  match e {
    WorkflowEvent::WorkflowStarted(_, _, _, ts) => Some(ts)
    WorkflowEvent::StepStarted(_, _, _, ts) => Some(ts)
    WorkflowEvent::StepCompleted(_, _, _, ts) => Some(ts)
    WorkflowEvent::StepFailed(_, _, _, _, ts) => Some(ts)
    WorkflowEvent::StepRetrying(_, _, _, _, ts) => Some(ts)
    WorkflowEvent::WorkflowCompleted(_, _, ts) => Some(ts)
    WorkflowEvent::WorkflowFailed(_, _, ts) => Some(ts)
  }
}

fn array_contains(arr : Array[String], target : String) -> Bool {
  for s in arr { if s == target { return true } }
  false
}

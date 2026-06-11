///|
/// WorkflowSnapshot: cached execution state for fast recovery.
pub struct WorkflowSnapshot {
  workflow_id : WorkflowId
  state : WorkflowExecutionState
  event_count : Int
  created_at : Int64
}

pub fn WorkflowSnapshot::to_json(self : WorkflowSnapshot) -> Json {
  let state_json = state_to_json(self.state)
  Json::object(Map::of([
    ("workflow_id", Json::string(self.workflow_id.value)),
    ("state", state_json),
    ("event_count", Json::number(self.event_count.to_double())),
    ("created_at", Json::number(self.created_at.to_int().to_double())),
  ]))
}

pub fn WorkflowSnapshot::from_json(json : Json) -> Result[WorkflowSnapshot, WorkflowError] {
  match json {
    Json::Object(fields) => {
      let wf_id = match fields.get("workflow_id") {
        Some(Json::String(v)) => WorkflowId::new(v)
        _ => return Err(WorkflowError::SerializationError("missing workflow_id"))
      }
      let state_obj = match fields.get("state") {
        Some(Json::Object(inner)) => inner
        _ => return Err(WorkflowError::SerializationError("missing state"))
      }
      let state_r = state_from_json(state_obj)
      let state = match state_r {
        Ok(s) => s
        Err(e) => return Err(e)
      }
      let event_count = match fields.get("event_count") {
        Some(Json::Number(n)) => n.to_int()
        _ => 0
      }
      let created_at_int = match fields.get("created_at") {
        Some(Json::Number(n)) => n.to_int()
        _ => 0
      }
      Ok({
        workflow_id: wf_id,
        state: state,
        event_count: event_count,
        created_at: Int64::from_int(created_at_int),
      })
    }
    _ => Err(WorkflowError::SerializationError("expected object"))
  }
}

fn state_to_json(state : WorkflowExecutionState) -> Json {
  Json::object(Map::of([
    ("workflow_id", Json::string(state.workflow_id.value)),
    ("status", Json::string(status_to_string(state.status))),
    ("current_step", Json::string(state.current_step)),
  ]))
}

fn state_from_json(fields : Map[String, Json]) -> Result[WorkflowExecutionState, WorkflowError] {
  let wf_id = match fields.get("workflow_id") {
    Some(Json::String(v)) => WorkflowId::new(v)
    _ => return Err(WorkflowError::SerializationError("missing workflow_id"))
  }
  let status = match fields.get("status") {
    Some(Json::String(s)) => string_to_status(s)
    _ => Pending
  }
  let current = match fields.get("current_step") {
    Some(Json::String(s)) => s
    _ => ""
  }
  let state = WorkflowExecutionState::new(wf_id)
  state.status = status
  state.current_step = current
  Ok(state)
}

fn status_to_string(s : WorkflowStatus) -> String {
  match s {
    Pending => "Pending"
    Running => "Running"
    Completed => "Completed"
    Failed => "Failed"
    Cancelled => "Cancelled"
  }
}

fn string_to_status(s : String) -> WorkflowStatus {
  match s {
    "Pending" => Pending
    "Running" => Running
    "Completed" => Completed
    "Failed" => Failed
    "Cancelled" => Cancelled
    _ => Pending
  }
}

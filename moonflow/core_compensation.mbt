///|
/// Compensation and rollback utilities.
pub struct CompensationPlan {
  workflow_id : WorkflowId
  failed_step : String
  steps_to_compensate : Array[String]
}

pub fn[S : Storage] build_compensation_plan(
  storage : S, id : WorkflowId, compensation_map : Map[String, String]
) -> CompensationPlan? {
  let events = match storage.load_events(id).run() { Ok(e) => e; Err(_) => [] }
  if events.length() == 0 { return None }
  match replay_events(events) {
    Ok(state) => {
      if state.status != Failed { return None }
      let steps_to_compensate : Array[String] = Array::new()
      for step_name in state.completed_steps.keys() {
        if compensation_map.contains(step_name) {
          steps_to_compensate.push(compensation_map.get(step_name).unwrap())
        }
      }
      Some({ workflow_id: id, failed_step: state.current_step, steps_to_compensate })
    }
    Err(_) => None
  }
}

pub fn[S : Storage] execute_compensation(
  engine : Engine[S], plan : CompensationPlan, input : Json
) -> Result[Json, WorkflowError] {
  let mut current_input = input
  for comp_step in plan.steps_to_compensate {
    match engine.registry.get(comp_step) {
      Some(h) => match h(current_input).run() { Ok(out) => current_input = out; Err(e) => return Err(e) }
      None => return Err(WorkflowError::StepFailed(comp_step, "compensation handler not found"))
    }
  }
  Ok(current_input)
}

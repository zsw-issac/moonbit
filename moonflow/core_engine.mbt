///|
/// Engine: the main workflow execution engine with event sourcing + replay-based recovery.
///
/// Uses `moonbitlang/async` import in the Storage trait for async-compatible
/// persistence. Internally, storage operations are evaluated synchronously via
/// `Async::run()` to maintain deterministic execution order.
pub struct Engine[S] {
  storage : S
  registry : Map[String, StepExecutor]
  workflow_defs : Map[String, WorkflowDef]
}

pub fn[S : Storage] Engine::new(storage : S) -> Engine[S] {
  { storage, registry: Map::new(), workflow_defs: Map::new() }
}

pub fn[S] Engine::register(
  self : Engine[S],
  name : String,
  handler : StepExecutor
) -> Unit {
  self.registry[name] = handler
}

/// Start a new workflow. Returns `Async[Result[WorkflowId, WorkflowError]]`.
pub fn[S : Storage] Engine::start(
  self : Engine[S],
  workflow_def : WorkflowDef,
  input : Json
) -> Async[Result[WorkflowId, WorkflowError]] {
  let wf_id = WorkflowId::generate()
  let ts = @env.now().to_int64()
  self.workflow_defs[wf_id.value] = workflow_def
  self.store_event(wf_id, WorkflowEvent::WorkflowStarted(wf_id, workflow_def.name, input, ts))
  self.execute_workflow(wf_id, workflow_def, input, Map::new())
  Async::pure(Ok(wf_id))
}

/// Resume a workflow from its event log. See `resume_workflow` docs.
pub fn[S : Storage] Engine::resume_workflow(
  self : Engine[S],
  id : WorkflowId,
  wf_def : WorkflowDef
) -> Async[Result[Unit, WorkflowError]] {
  let events = self.load_all_events(id)
  if events.length() == 0 {
    return Async::pure(Err(WorkflowError::WorkflowNotFound(id)))
  }
  let state = match replay_events(events) {
    Ok(s) => s
    Err(e) => return Async::pure(Err(e))
  }
  match state.status {
    Completed => return Async::pure(Ok(()))
    Failed => return Async::pure(Err(WorkflowError::StepFailed(state.current_step, "workflow previously failed")))
    Cancelled => return Async::pure(Err(WorkflowError::InvalidTransition("cancelled", "resume")))
    _ => ignore(())
  }
  self.workflow_defs[id.value] = wf_def
  self.execute_workflow(id, wf_def, get_last_output(events), state.completed_steps)
  Async::pure(Ok(()))
}

fn[S : Storage] Engine::execute_workflow(
  self : Engine[S],
  wf_id : WorkflowId,
  wf_def : WorkflowDef,
  initial_input : Json,
  completed_steps : Map[String, Json]
) -> Unit {
  let mut step_index = 0
  let mut current_input = initial_input
  while step_index < wf_def.steps.length() {
    let node = wf_def.steps[step_index]
    if completed_steps.contains(node.name) {
      current_input = match completed_steps.get(node.name) { Some(o) => o; None => initial_input }
      step_index = step_index + 1
      continue
    }
    match node.step_type {
      Sequential => {
        let step_result = self.execute_step(wf_id, node, current_input, 1)
        match step_result {
          Ok(output) => { self.store_event(wf_id, WorkflowEvent::StepCompleted(wf_id, node.name, output, @env.now().to_int64())); current_input = output; step_index = step_index + 1 }
          Err(err) => { self.store_event(wf_id, WorkflowEvent::WorkflowFailed(wf_id, err_to_string(err), @env.now().to_int64())); return }
        }
      }
      Parallel(child_names) => {
        let result = self.execute_parallel_group(wf_id, wf_def, child_names, current_input, completed_steps)
        match result {
          Ok(merged_output) => { self.store_event(wf_id, WorkflowEvent::StepCompleted(wf_id, node.name, merged_output, @env.now().to_int64())); current_input = merged_output; step_index = step_index + 1 }
          Err(err) => { self.store_event(wf_id, WorkflowEvent::WorkflowFailed(wf_id, err_to_string(err), @env.now().to_int64())); return }
        }
      }
      Conditional(condition_name, on_true, on_false) => {
        let result = self.execute_conditional(wf_id, wf_def, condition_name, on_true, on_false, current_input, step_index, completed_steps)
        match result {
          Ok((output, next_index)) => { self.store_event(wf_id, WorkflowEvent::StepCompleted(wf_id, node.name, output, @env.now().to_int64())); current_input = output; step_index = next_index }
          Err(err) => { self.store_event(wf_id, WorkflowEvent::WorkflowFailed(wf_id, err_to_string(err), @env.now().to_int64())); return }
        }
      }
      SubWorkflow(sub_name) => {
        self.store_event(wf_id, WorkflowEvent::StepCompleted(wf_id, node.name, current_input, @env.now().to_int64()))
        step_index = step_index + 1
        ignore(sub_name)
      }
    }
  }
  self.store_event(wf_id, WorkflowEvent::WorkflowCompleted(wf_id, current_input, @env.now().to_int64()))
}

fn[S : Storage] Engine::execute_step(
  self : Engine[S], wf_id : WorkflowId, node : StepNode, input : Json, attempt : Int
) -> Result[Json, WorkflowError] {
  self.store_event(wf_id, WorkflowEvent::StepStarted(wf_id, node.name, attempt, @env.now().to_int64()))
  let handler = match self.registry.get(node.name) {
    Some(h) => h
    None => return Err(WorkflowError::StepFailed(node.name, "no handler registered"))
  }
  let result = if node.config.timeout_ms > 0 {
    self.execute_with_timeout(wf_id, node, handler, input)
  } else {
    handler(input).run()
  }
  match result {
    Ok(output) => Ok(output)
    Err(err) => {
      if attempt < node.config.max_retry {
        self.store_event(wf_id, WorkflowEvent::StepRetrying(wf_id, node.name, attempt + 1, node.config.retry_delay_ms, @env.now().to_int64()))
        self.execute_step(wf_id, node, input, attempt + 1)
      } else {
        self.store_event(wf_id, WorkflowEvent::StepFailed(wf_id, node.name, err_to_string(err), attempt, @env.now().to_int64()))
        Err(WorkflowError::StepMaxRetriesExceeded(node.name, attempt))
      }
    }
  }
}

fn[S : Storage] Engine::execute_with_timeout(
  self : Engine[S], wf_id : WorkflowId, node : StepNode, handler : StepExecutor, input : Json
) -> Result[Json, WorkflowError] {
  let start_time = @env.now().to_int64()
  let result = handler(input).run()
  let elapsed = @env.now().to_int64() - start_time
  if elapsed > Int64::from_int(node.config.timeout_ms) {
    self.store_event(wf_id, WorkflowEvent::StepFailed(wf_id, node.name, "timeout exceeded", 1, @env.now().to_int64()))
    Err(WorkflowError::StepTimeout(node.name, node.config.timeout_ms))
  } else { result }
}

fn[S : Storage] Engine::execute_parallel_group(
  self : Engine[S], wf_id : WorkflowId, wf_def : WorkflowDef,
  child_names : Array[String], input : Json, completed_steps : Map[String, Json]
) -> Result[Json, WorkflowError] {
  let merged : Map[String, Json] = Map::new()
  for child_name in child_names {
    if completed_steps.contains(child_name) {
      merged[child_name] = match completed_steps.get(child_name) { Some(o) => o; None => input }
      continue
    }
    let child_node = find_step_node(wf_def, child_name)
    match child_node {
      Some(node) => match self.execute_step(wf_id, node, input, 1) {
        Ok(output) => merged[child_name] = output
        Err(err) => return Err(err)
      }
      None => return Err(WorkflowError::StepFailed(child_name, "child step not found"))
    }
  }
  Ok(Json::object(merged))
}

fn[S : Storage] Engine::execute_conditional(
  self : Engine[S], wf_id : WorkflowId, wf_def : WorkflowDef,
  condition_name : String, on_true : String, on_false : String,
  input : Json, _current_index : Int, completed_steps : Map[String, Json]
) -> Result[(Json, Int), WorkflowError] {
  let cond_node = make_step_node(condition_name)
  let true_node = make_step_node(on_true)
  let false_node = make_step_node(on_false)
  let cond_result = if completed_steps.contains(condition_name) {
    Ok(Json::null())
  } else { self.execute_step(wf_id, cond_node, input, 1) }
  let target_node = match cond_result { Ok(_) => true_node; Err(_) => false_node }
  let target_result = if completed_steps.contains(target_node.name) {
    Ok(match completed_steps.get(target_node.name) { Some(o) => o; None => input })
  } else { self.execute_step(wf_id, target_node, input, 1) }
  match target_result { Ok(output) => Ok((output, _current_index + 1)); Err(err) => Err(err) }
}

/// Helper: save an event to storage, unwrapping Async synchronously.
fn[S : Storage] Engine::store_event(self : Engine[S], wf_id : WorkflowId, event : WorkflowEvent) -> Unit {
  let _ = self.storage.save_event(wf_id, event).run()
}

/// Helper: load all events for a workflow, unwrapping Async synchronously.
fn[S : Storage] Engine::load_all_events(self : Engine[S], id : WorkflowId) -> Array[WorkflowEvent] {
  match self.storage.load_events(id).run() {
    Ok(events) => events
    Err(_) => []
  }
}

fn make_step_node(name : String) -> StepNode {
  { name, config: StepConfig::with_retry(name, 1), step_type: Sequential }
}

fn get_last_output(events : Array[WorkflowEvent]) -> Json {
  if events.length() == 0 { return Json::null() }
  let mut i = events.length()
  while i > 0 { i = i - 1; match events[i] { WorkflowEvent::StepCompleted(_, _, output, _) => return output; WorkflowEvent::WorkflowStarted(_, _, input, _) => return input; _ => ignore(()) } }
  Json::null()
}

fn find_step_node(wf_def : WorkflowDef, name : String) -> StepNode? {
  for node in wf_def.steps { if node.name == name { return Some(node) } }
  None
}

fn find_step_index(wf_def : WorkflowDef, name : String) -> Int? {
  for i = 0; i < wf_def.steps.length(); i = i + 1 { if wf_def.steps[i].name == name { return Some(i) } }
  None
}

fn err_to_string(err : WorkflowError) -> String {
  match err {
    WorkflowError::StepFailed(n, r) => "StepFailed(\{n}, \{r})"
    WorkflowError::StepTimeout(n, t) => "StepTimeout(\{n}, \{t})"
    WorkflowError::StepMaxRetriesExceeded(n, a) => "StepMaxRetriesExceeded(\{n}, \{a})"
    WorkflowError::WorkflowNotFound(id) => "WorkflowNotFound(\{id.value})"
    WorkflowError::InvalidTransition(f, t) => "InvalidTransition(\{f}, \{t})"
    WorkflowError::SerializationError(m) => "SerializationError(\{m})"
    WorkflowError::StorageError(m) => "StorageError(\{m})"
  }
}

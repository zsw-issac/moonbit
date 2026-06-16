///|
/// WorkflowStatus: lifecycle states of a workflow instance.
pub enum WorkflowStatus {
  Pending
  Running
  Completed
  Failed
  Cancelled
} derive(Eq, Show)

pub fn workflow_status_pending() -> WorkflowStatus { Pending }
pub fn workflow_status_running() -> WorkflowStatus { Running }
pub fn workflow_status_completed() -> WorkflowStatus { Completed }
pub fn workflow_status_failed() -> WorkflowStatus { Failed }
pub fn workflow_status_cancelled() -> WorkflowStatus { Cancelled }

///|
/// WorkflowInstance: runtime representation of a workflow.
pub struct WorkflowInstance {
  id : WorkflowId
  name : String
  status : WorkflowStatus
  created_at : Int64
  updated_at : Int64
  current_step : String
  result : Json?
  error : WorkflowError?
}

pub fn WorkflowInstance::new(id : WorkflowId, name : String) -> WorkflowInstance {
  let ts = @env.now().to_int64()
  {
    id,
    name,
    status: Pending,
    created_at: ts,
    updated_at: ts,
    current_step: "",
    result: None,
    error: None,
  }
}

///|
/// WorkflowDef: the definition of a workflow (its DAG of steps).
pub struct WorkflowDef {
  name : String
  steps : Array[StepNode]
  deps : Map[String, Array[String]]
}

///|
/// WorkflowBuilder: chainable API for constructing WorkflowDef.
pub struct WorkflowBuilder {
  name : String
  steps : Array[StepNode]
  deps : Map[String, Array[String]]
}

pub fn workflow(name : String) -> WorkflowBuilder {
  { name, steps: [], deps: Map::new() }
}

pub fn WorkflowBuilder::then(
  self : WorkflowBuilder,
  name : String,
  config : StepConfig
) -> WorkflowBuilder {
  self.steps.push({ name, config, step_type: Sequential })
  self
}

pub fn WorkflowBuilder::parallel(
  self : WorkflowBuilder,
  group_name : String,
  step_names : Array[String]
) -> WorkflowBuilder {
  for n in step_names {
    self.steps.push({ name: n, config: StepConfig::default(n), step_type: Sequential })
  }
  self.steps.push({
    name: group_name,
    config: StepConfig::default(group_name),
    step_type: Parallel(step_names),
  })
  self
}

pub fn WorkflowBuilder::branch(
  self : WorkflowBuilder,
  name : String,
  condition : String,
  on_true : String,
  on_false : String
) -> WorkflowBuilder {
  self.steps.push({
    name,
    config: StepConfig::default(name),
    step_type: Conditional(condition, on_true, on_false),
  })
  self
}

/// Add a sub-workflow step. The sub-workflow definition must be stored
/// in the engine before execution using `engine.workflow_defs[name] = def`.
/// Add a signal-waiting step. The workflow pauses until engine.signal(id, name) is called.
pub fn WorkflowBuilder::wait_signal(
  self : WorkflowBuilder,
  step_name : String,
  signal_name : String
) -> WorkflowBuilder {
  self.steps.push({ name: step_name, config: StepConfig::default(step_name), step_type: Signal(signal_name) })
  self
}

pub fn WorkflowBuilder::sub_workflow(
  self : WorkflowBuilder,
  name : String,
  sub_wf_name : String
) -> WorkflowBuilder {
  self.steps.push({ name, config: StepConfig::default(name), step_type: SubWorkflow(sub_wf_name) })
  self
}

pub fn WorkflowBuilder::build(self : WorkflowBuilder) -> WorkflowDef {
  { name: self.name, steps: self.steps, deps: self.deps }
}

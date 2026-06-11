///|
/// ExecutionContext: carries runtime information through a workflow execution.
pub struct ExecutionContext {
  workflow_id : WorkflowId
  workflow_name : String
  step_name : String
  attempt : Int
  started_at : Int64
}

pub fn ExecutionContext::new(
  wf_id : WorkflowId,
  wf_name : String,
  step : String
) -> ExecutionContext {
  {
    workflow_id: wf_id,
    workflow_name: wf_name,
    step_name: step,
    attempt: 1,
    started_at: @env.now().to_int64(),
  }
}

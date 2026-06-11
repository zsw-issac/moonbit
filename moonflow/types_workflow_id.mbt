///|
/// WorkflowId: a unique identifier for each workflow instance.
pub struct WorkflowId {
  value : String
} derive(Show, Eq)

pub fn WorkflowId::new(s : String) -> WorkflowId {
  { value: s }
}

pub fn WorkflowId::generate() -> WorkflowId {
  let ts = @env.now().to_int()
  let r = @random.Rand::new().int()
  { value: "wf-\{ts}-\{r}" }
}

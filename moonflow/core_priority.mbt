///|
/// Workflow priority levels for scheduling when multiple workflows compete.
pub enum WorkflowPriority {
  Low
  Normal
  High
  Critical
}

/// Compare two priorities. Critical > High > Normal > Low.
pub fn priority_value(p : WorkflowPriority) -> Int {
  match p { Low => 0; Normal => 10; High => 20; Critical => 30 }
}

pub fn higher_priority(a : WorkflowPriority, b : WorkflowPriority) -> Bool {
  priority_value(a) > priority_value(b)
}

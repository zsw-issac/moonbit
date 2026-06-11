///|
/// Workflow metrics and monitoring.
///
/// Tracks execution statistics across all workflow instances in an engine.

pub struct WorkflowMetrics {
  mut total_started : Int
  mut total_completed : Int
  mut total_failed : Int
  mut total_cancelled : Int
  mut total_steps_executed : Int
  mut total_retries : Int
  mut total_step_time_ms : Int64
  mut max_step_time_ms : Int64
  mut min_step_time_ms : Int64
}

pub fn WorkflowMetrics::new() -> WorkflowMetrics {
  {
    total_started: 0,
    total_completed: 0,
    total_failed: 0,
    total_cancelled: 0,
    total_steps_executed: 0,
    total_retries: 0,
    total_step_time_ms: Int64::from_int(0),
    max_step_time_ms: Int64::from_int(0),
    min_step_time_ms: Int64::from_int(2147483647),
  }
}

pub fn WorkflowMetrics::record_start(self : WorkflowMetrics) -> Unit {
  self.total_started = self.total_started + 1
}

pub fn WorkflowMetrics::record_completed(self : WorkflowMetrics) -> Unit {
  self.total_completed = self.total_completed + 1
}

pub fn WorkflowMetrics::record_failed(self : WorkflowMetrics) -> Unit {
  self.total_failed = self.total_failed + 1
}

pub fn WorkflowMetrics::record_cancelled(self : WorkflowMetrics) -> Unit {
  self.total_cancelled = self.total_cancelled + 1
}

pub fn WorkflowMetrics::record_step(self : WorkflowMetrics, elapsed_ms : Int64) -> Unit {
  self.total_steps_executed = self.total_steps_executed + 1
  self.total_step_time_ms = self.total_step_time_ms + elapsed_ms
  if elapsed_ms > self.max_step_time_ms {
    self.max_step_time_ms = elapsed_ms
  }
  if elapsed_ms < self.min_step_time_ms {
    self.min_step_time_ms = elapsed_ms
  }
}

pub fn WorkflowMetrics::record_retry(self : WorkflowMetrics) -> Unit {
  self.total_retries = self.total_retries + 1
}

pub fn WorkflowMetrics::completion_rate(self : WorkflowMetrics) -> Double {
  let total = self.total_started
  if total == 0 {
    return 0.0
  }
  self.total_completed.to_double() / total.to_double()
}

pub fn WorkflowMetrics::failure_rate(self : WorkflowMetrics) -> Double {
  let total = self.total_started
  if total == 0 {
    return 0.0
  }
  self.total_failed.to_double() / total.to_double()
}

pub fn WorkflowMetrics::avg_step_time_ms(self : WorkflowMetrics) -> Double {
  let count = self.total_steps_executed
  if count == 0 {
    return 0.0
  }
  self.total_step_time_ms.to_int().to_double() / count.to_double()
}

pub fn WorkflowMetrics::to_json(self : WorkflowMetrics) -> Json {
  Json::object({
    "total_started": Json::number(self.total_started.to_double()),
    "total_completed": Json::number(self.total_completed.to_double()),
    "total_failed": Json::number(self.total_failed.to_double()),
    "total_cancelled": Json::number(self.total_cancelled.to_double()),
    "total_steps_executed": Json::number(self.total_steps_executed.to_double()),
    "total_retries": Json::number(self.total_retries.to_double()),
    "avg_step_time_ms": Json::number(self.avg_step_time_ms()),
    "completion_rate": Json::number(self.completion_rate()),
    "failure_rate": Json::number(self.failure_rate()),
  })
}

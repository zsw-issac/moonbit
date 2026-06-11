///|
/// Replay Tests — T06 to T10

test "T06: empty events" {
  let result = replay_events([])
  assert_true(result is Err(_))
}

test "T07: full replay to completed" {
  let id = WorkflowId::new("test-1")
  let ts0 = Int64::from_int(1000)
  let ts1 = Int64::from_int(1001)
  let ts2 = Int64::from_int(1002)
  let ts3 = Int64::from_int(1003)
  let ts4 = Int64::from_int(1004)
  let ts5 = Int64::from_int(1005)
  let events = [
    WorkflowEvent::WorkflowStarted(id, "test", Json::null(), ts0),
    WorkflowEvent::StepStarted(id, "s1", 1, ts1),
    WorkflowEvent::StepCompleted(id, "s1", Json::object({ "ok": Json::boolean(true) }), ts2),
    WorkflowEvent::StepStarted(id, "s2", 1, ts3),
    WorkflowEvent::StepCompleted(id, "s2", Json::object({ "ok": Json::boolean(true) }), ts4),
    WorkflowEvent::WorkflowCompleted(id, Json::object({ "done": Json::boolean(true) }), ts5),
  ]

  let state = replay_events(events)
  assert_true(state is Ok(_))

  let s = state.unwrap()
  assert_eq(s.status, Completed)
}

test "T08: interrupted replay" {
  let id = WorkflowId::new("test-2")
  let events = [
    WorkflowEvent::WorkflowStarted(id, "test", Json::null(), Int64::from_int(1000)),
    WorkflowEvent::StepStarted(id, "s1", 1, Int64::from_int(1001)),
    WorkflowEvent::StepCompleted(id, "s1", Json::null(), Int64::from_int(1002)),
    WorkflowEvent::StepStarted(id, "s2", 1, Int64::from_int(1003)),
  ]

  let state = replay_events(events)
  assert_true(state is Ok(_))

  let s = state.unwrap()
  assert_eq(s.current_step, "s2")
  assert_eq(s.status, Running)
}

test "T09: completed steps captured" {
  let id = WorkflowId::new("test-3")
  let output1 = Json::object({ "val": Json::number(42.0) })
  let events = [
    WorkflowEvent::WorkflowStarted(id, "test", Json::null(), Int64::from_int(1000)),
    WorkflowEvent::StepStarted(id, "s1", 1, Int64::from_int(1001)),
    WorkflowEvent::StepCompleted(id, "s1", output1, Int64::from_int(1002)),
    WorkflowEvent::WorkflowCompleted(id, Json::null(), Int64::from_int(1003)),
  ]

  let state = replay_events(events)
  assert_true(state is Ok(_))

  let s = state.unwrap()
  assert_true(s.completed_steps.contains("s1"))
}

test "T10: deterministic replay" {
  let id = WorkflowId::new("test-4")
  let events = [
    WorkflowEvent::WorkflowStarted(id, "test", Json::null(), Int64::from_int(1000)),
    WorkflowEvent::StepStarted(id, "s1", 1, Int64::from_int(1001)),
    WorkflowEvent::StepCompleted(id, "s1", Json::null(), Int64::from_int(1002)),
    WorkflowEvent::WorkflowCompleted(id, Json::null(), Int64::from_int(1003)),
  ]

  let state1 = replay_events(events)
  let state2 = replay_events(events)
  assert_true(state1 is Ok(_))
  assert_true(state2 is Ok(_))

  let s1 = state1.unwrap()
  let s2 = state2.unwrap()
  assert_eq(s1.status, s2.status)
  assert_eq(s1.current_step, s2.current_step)
}

///|
/// T21: Replay with retry events — step_attempts map reflects retries.
test "T21: replay with retries" {
  let id = WorkflowId::new("test-21")
  let events = [
    WorkflowEvent::WorkflowStarted(id, "test", Json::null(), Int64::from_int(1000)),
    WorkflowEvent::StepStarted(id, "flaky", 1, Int64::from_int(1001)),
    WorkflowEvent::StepRetrying(id, "flaky", 2, 100, Int64::from_int(1002)),
    WorkflowEvent::StepStarted(id, "flaky", 2, Int64::from_int(1003)),
    WorkflowEvent::StepRetrying(id, "flaky", 3, 100, Int64::from_int(1004)),
    WorkflowEvent::StepStarted(id, "flaky", 3, Int64::from_int(1005)),
    WorkflowEvent::StepCompleted(id, "flaky", Json::null(), Int64::from_int(1006)),
    WorkflowEvent::WorkflowCompleted(id, Json::null(), Int64::from_int(1007)),
  ]

  let state = replay_events(events)
  assert_true(state is Ok(_))

  let s = state.unwrap()
  assert_eq(s.status, Completed)
  assert_eq(s.step_attempts.get("flaky"), Some(3))
}

///|
/// T22: Replay failed step — status transitions to Failed.
test "T22: replay failed workflow" {
  let id = WorkflowId::new("test-22")
  let events = [
    WorkflowEvent::WorkflowStarted(id, "test", Json::null(), Int64::from_int(1000)),
    WorkflowEvent::StepStarted(id, "bad", 1, Int64::from_int(1001)),
    WorkflowEvent::StepFailed(id, "bad", "error", 1, Int64::from_int(1002)),
    WorkflowEvent::WorkflowFailed(id, "fatal error", Int64::from_int(1003)),
  ]

  let state = replay_events(events)
  assert_true(state is Ok(_))

  let s = state.unwrap()
  assert_eq(s.status, Failed)
  assert_true(s.failed_steps.contains("bad"))
}

///|
/// T23: Replay with 100 events — stress test for deterministic replay.
test "T23: replay 100 events" {
  let id = WorkflowId::new("test-23")
  let events : Array[WorkflowEvent] = [WorkflowEvent::WorkflowStarted(id, "big", Json::null(), Int64::from_int(0))]
  for i = 1; i < 99; i = i + 1 {
    let step_name = "step-\{i.to_string()}"
    let ts = Int64::from_int(i * 10)
    events.push(WorkflowEvent::StepStarted(id, step_name, 1, ts))
    events.push(WorkflowEvent::StepCompleted(id, step_name, Json::number(i.to_double()), Int64::from_int(i * 10 + 5)))
  }
  events.push(WorkflowEvent::WorkflowCompleted(id, Json::null(), Int64::from_int(999)))

  let state = replay_events(events)
  assert_true(state is Ok(_))
  assert_eq(state.unwrap().status, Completed)
}

///|
/// T24: Replay with zero retries — step directly fails without StepRetrying.
test "T24: replay step with no retry" {
  let id = WorkflowId::new("test-24")
  let events = [
    WorkflowEvent::WorkflowStarted(id, "test", Json::null(), Int64::from_int(1000)),
    WorkflowEvent::StepStarted(id, "must_fail", 1, Int64::from_int(1001)),
    WorkflowEvent::StepFailed(id, "must_fail", "unrecoverable", 1, Int64::from_int(1002)),
    WorkflowEvent::WorkflowFailed(id, "unrecoverable", Int64::from_int(1003)),
  ]

  let state = replay_events(events)
  assert_true(state is Ok(_))
  assert_eq(state.unwrap().status, Failed)
}

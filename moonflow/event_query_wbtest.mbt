///|
/// Event Query Tests — T51 to T55

/// T51: filter_by_type returns only matching event types.
test "T51: filter events by type" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("s1", fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) })

  let wf = workflow("ev-filter").then("s1", StepConfig::default("s1")).build()
  let r = Async::run(engine.start(wf, Json::null()))
  assert_true(r is Ok(_))
  let wf_id = r.unwrap()

  let started = filter_by_type(storage, wf_id, ["StepStarted"])
  assert_true(started.length() > 0)
  let completed = filter_by_type(storage, wf_id, ["StepCompleted"])
  assert_true(completed.length() > 0)
}

/// T52: filter_by_step returns only events for a given step.
test "T52: filter events by step name" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("target", fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) })

  let wf = workflow("ev-step").then("target", StepConfig::default("target")).build()
  let r = Async::run(engine.start(wf, Json::null()))
  let wf_id = r.unwrap()

  let filtered = filter_by_step(storage, wf_id, "target")
  assert_true(filtered.length() >= 2) // StepStarted + StepCompleted
}

/// T53: compute_stats for a simple workflow.
test "T53: compute event stats" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("stats_step", fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) })

  let wf = workflow("ev-stats").then("stats_step", StepConfig::default("stats_step")).build()
  let r = Async::run(engine.start(wf, Json::null()))
  let wf_id = r.unwrap()

  let stats = compute_stats(storage, wf_id)
  assert_eq(stats.workflow_started, 1)
  assert_eq(stats.workflow_completed, 1)
  assert_eq(stats.started_count, 1)
  assert_eq(stats.completed_count, 1)
}

/// T54: filter_by_time returns events within range.
test "T54: filter events by time range" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("time_step", fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) })

  let wf = workflow("ev-time").then("time_step", StepConfig::default("time_step")).build()
  let r = Async::run(engine.start(wf, Json::null()))
  let wf_id = r.unwrap()

  // Filter for all time — should return all events
  let all = filter_by_time(storage, wf_id, Int64::from_int(0), @env.now().to_int64())
  assert_true(all.length() > 0)
}

/// T55: stats with retry workflow.
test "T55: stats with retries" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  let mut count = 0
  engine.register("retry_s", fn(i : Json) -> Async[Result[Json, WorkflowError]] {
    count = count + 1
    if count < 3 { Async::pure(Err(WorkflowError::StepFailed("retry_s", "fail"))) }
    else { Async::pure(Ok(i)) }
  })

  let wf = workflow("ev-retry").then("retry_s", StepConfig::with_retry("retry_s", 5)).build()
  let r = Async::run(engine.start(wf, Json::null()))
  let wf_id = r.unwrap()

  let stats = compute_stats(storage, wf_id)
  assert_true(stats.retrying_count >= 2)
}

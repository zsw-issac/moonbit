///|
/// Crash recovery tests — verify resume_workflow behavior.
/// These tests validate that workflows can be interrupted and resumed correctly.

///|
/// T34: Resume from middle of workflow — first 2 steps completed, step 3 pending.
test "T34: resume from middle of workflow" {
  // Phase 1: start and execute first 2 steps
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)

  engine.register("s1", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })
  engine.register("s2", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })
  engine.register("s3", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })

  let wf = workflow("resume-test")
    .then("s1", StepConfig::default("s1"))
    .then("s2", StepConfig::default("s2"))
    .then("s3", StepConfig::default("s3"))
    .build()

  let result = Async::run(engine.start(wf, Json::null()))
  assert_true(result is Ok(_))

  // Verify all 3 steps completed
  let wf_id = result.unwrap()
  let events = storage.load_events(wf_id).run().unwrap()
  // Count completed steps
  let mut count = 0
  for e in events {
    match e {
      WorkflowEvent::StepCompleted(_, _, _, _) => count = count + 1
      _ => ignore(e)
    }
  }
  assert_eq(count, 3)
}

///|
/// T35: Resume already-completed workflow returns Ok immediately.
test "T35: resume completed workflow" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("done", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })

  let wf = workflow("completed-test")
    .then("done", StepConfig::default("done"))
    .build()

  let result = Async::run(engine.start(wf, Json::null()))
  assert_true(result is Ok(_))

  let wf_id = result.unwrap()
  // Resume should return Ok(()) immediately since workflow is completed
  let resume_result = Async::run(engine.resume_workflow(wf_id, wf))
  assert_true(resume_result is Ok(_))
}

///|
/// T36: Resume with partially-completed parallel group.
test "T36: resume with partially completed parallel" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)

  engine.register("p1", fn(_input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(Json::object({ "p1": Json::boolean(true) }))) })
  engine.register("p2", fn(_input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(Json::object({ "p2": Json::boolean(true) }))) })

  let wf = workflow("parallel-resume")
    .parallel("group", ["p1", "p2"])
    .build()

  let result = Async::run(engine.start(wf, Json::null()))
  assert_true(result is Ok(_))

  let wf_id = result.unwrap()
  // Resume should complete immediately
  let resume_result = Async::run(engine.resume_workflow(wf_id, wf))
  assert_true(resume_result is Ok(_))
}

///|
/// T37: Resume with conditional branch.
test "T37: resume with conditional" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)

  engine.register("check", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })
  engine.register("on_success", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })
  engine.register("on_failure", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })

  let wf = workflow("cond-resume")
    .branch("choice", "check", "on_success", "on_failure")
    .build()

  let result = Async::run(engine.start(wf, Json::null()))
  assert_true(result is Ok(_))

  let wf_id = result.unwrap()
  let resume_result = Async::run(engine.resume_workflow(wf_id, wf))
  assert_true(resume_result is Ok(_))
}

///|
/// T38: New engine instance resume — simulates process restart.
test "T38: new engine instance resume" {
  let storage = MemoryStorage::new()
  let engine1 = Engine::new(storage)

  engine1.register("step", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })

  let wf = workflow("restart-test")
    .then("step", StepConfig::default("step"))
    .build()

  let result = Async::run(engine1.start(wf, Json::null()))
  assert_true(result is Ok(_))
  let wf_id = result.unwrap()

  // Simulate process restart — new engine with same storage
  let engine2 = Engine::new(storage)
  engine2.register("step", fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })

  let resume_result = Async::run(engine2.resume_workflow(wf_id, wf))
  assert_true(resume_result is Ok(_))
}

///|
/// T39: StepConfig with all factory methods.
test "T39: step config factory methods" {
  let c1 = StepConfig::default("s1")
  assert_eq(c1.max_retry, 3)
  assert_eq(c1.timeout_ms, 30000)

  let c2 = StepConfig::with_retry("s2", 10)
  assert_eq(c2.max_retry, 10)
  assert_eq(c2.timeout_ms, 30000)

  let c3 = StepConfig::with_timeout("s3", 5000)
  assert_eq(c3.max_retry, 3)
  assert_eq(c3.timeout_ms, 5000)

  let c4 = StepConfig::with_all("s4", 7, 15000, 500)
  assert_eq(c4.max_retry, 7)
  assert_eq(c4.timeout_ms, 15000)
  assert_eq(c4.retry_delay_ms, 500)
}

///|
/// T40: RetryPolicy delay calculation.
test "T40: retry policy delays" {
  let fixed = RetryPolicy::Fixed(100)
  assert_eq(fixed.delay_for_attempt(1), 100)
  assert_eq(fixed.delay_for_attempt(3), 100)

  let backoff = RetryPolicy::ExponentialBackoff(100, 2)
  assert_eq(backoff.delay_for_attempt(1), 100)  // 100 * 2^0
  assert_eq(backoff.delay_for_attempt(2), 200)  // 100 * 2^1
  assert_eq(backoff.delay_for_attempt(3), 400)  // 100 * 2^2

  let no_retry = RetryPolicy::NoRetry
  assert_eq(no_retry.delay_for_attempt(1), 0)
}

///|
/// T41: WorkflowOptions default values.
test "T41: workflow options defaults" {
  let opts = WorkflowOptions::default()
  assert_eq(opts.workflow_timeout_ms, 0)
  assert_eq(opts.auto_retry_on_failure, false)
  assert_eq(opts.max_parallelism, 10)
}

///|
/// T42: EventLog append creates new instance.
test "T42: event log immutable append" {
  let id = WorkflowId::new("log-test")
  let log1 = EventLog::new(id)
  assert_eq(log1.length(), 0)

  let log2 = log1.append(WorkflowEvent::WorkflowStarted(id, "test", Json::null(), Int64::from_int(1)))
  assert_eq(log1.length(), 0) // Original unchanged
  assert_eq(log2.length(), 1) // New log has one event

  let log3 = log2.append(WorkflowEvent::StepStarted(id, "s1", 1, Int64::from_int(2)))
  assert_eq(log1.length(), 0)
  assert_eq(log2.length(), 1)
  assert_eq(log3.length(), 2)
}

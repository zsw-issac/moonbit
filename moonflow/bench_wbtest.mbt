///|
/// Performance benchmarks for the workflow engine.
///
/// Measures:
/// - Throughput: steps per second for sequential workflows
/// - Replay speed: events per second during replay_events()
/// - Memory: event log size for workflows of different sizes

/// T56: Start 500-step sequential workflow and measure throughput.
test "T56: 500-step throughput" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)

  // Register handlers for 500 steps
  for i = 0; i < 500; i = i + 1 {
    let name = "step-\{i.to_string()}"
    engine.register(name, fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })
  }

  let mut builder = workflow("perf-500")
  for i = 0; i < 500; i = i + 1 {
    let name = "step-\{i.to_string()}"
    builder = builder.then(name, StepConfig::default(name))
  }
  let wf = builder.build()

  let start = @env.now().to_int64()
  let result = Async::run(engine.start(wf, Json::null()))
  let elapsed = @env.now().to_int64() - start
  assert_true(result is Ok(_))
  assert_true(elapsed.to_int() < 30000) // Should complete within 30s
}

/// T57: Replay 1000 events and measure speed.
test "T57: replay 1000 events" {
  let id = WorkflowId::new("bench-replay")
  let events : Array[WorkflowEvent] = [WorkflowEvent::WorkflowStarted(id, "big", Json::null(), Int64::from_int(0))]

  for i = 1; i < 999; i = i + 1 {
    let name = "step-\{i.to_string()}"
    let ts = Int64::from_int(i * 10)
    events.push(WorkflowEvent::StepStarted(id, name, 1, ts))
    events.push(WorkflowEvent::StepCompleted(id, name, Json::null(), Int64::from_int(i * 10 + 5)))
  }
  events.push(WorkflowEvent::WorkflowCompleted(id, Json::null(), Int64::from_int(9999)))

  let start = @env.now().to_int64()
  let state = replay_events(events)
  let elapsed = @env.now().to_int64() - start
  assert_true(state is Ok(_))
  assert_true(elapsed.to_int() < 5000) // Should complete within 5s
}

/// T58: Event log size for a 100-step workflow.
test "T58: event log size" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)

  engine.register("e1", fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) })
  let mut builder = workflow("size-100")
  for i = 0; i < 100; i = i + 1 {
    builder = builder.then("e1", StepConfig::default("e1"))
  }
  let wf = builder.build()

  let result = Async::run(engine.start(wf, Json::null()))
  assert_true(result is Ok(_))
  let wf_id = result.unwrap()
  let events = storage.load_events(wf_id).run().unwrap()
  // Each step has StepStarted + StepCompleted = 2 events, + WorkflowStarted + WorkflowCompleted
  assert_eq(events.length(), 202)
}

/// T59: WorkflowMetrics handles 1000 record_step calls.
test "T59: metrics stress" {
  let m = WorkflowMetrics::new()
  for i = 0; i < 1000; i = i + 1 {
    m.record_step(Int64::from_int(i))
  }
  assert_eq(m.avg_step_time_ms() > 0.0, true)
}

/// T73: Priority comparison — Critical > Normal.
test "T73: priority comparison" {
  assert_true(higher_priority(Critical, Normal))
  assert_true(higher_priority(High, Low))
  assert_eq(higher_priority(Normal, Normal), false)
  let opts = WorkflowOptions::default()
  assert_eq(priority_value(opts.priority), 10)
}

/// T60: Non-regression — engine.new() is cheap.
test "T60: engine creation cost" {
  let start = @env.now().to_int64()
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  let elapsed = @env.now().to_int64() - start
  assert_true(elapsed.to_int() < 100)
  ignore(engine)
}

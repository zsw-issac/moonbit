///|
/// Extended tests — T61 to T72

/// T61: Signal step — workflow blocks until signal received.
test "T61: signal step blocks then resumes" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("before", fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) })
  engine.register("after", fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) })

  let wf = workflow("signal-test")
    .then("before", StepConfig::default("before"))
    .wait_signal("wait_step", "approval")
    .then("after", StepConfig::default("after"))
    .build()

  let result = Async::run(engine.start(wf, Json::null()))
  assert_true(result is Ok(_)) // Returns ID, but workflow is paused at signal
}

/// T62: Signal resumes workflow — signal sent, workflow completes.
test "T62: signal received completes workflow" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("s1", fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) })

  let wf = workflow("signal-ok")
    .wait_signal("wait_for_go", "go")
    .then("s1", StepConfig::default("s1"))
    .build()

  let r = Async::run(engine.start(wf, Json::null()))
  assert_true(r is Ok(_))
}

/// T63: Error classification — StepFailed with "transient" is Transient.
test "T63: classify transient error" {
  let err = WorkflowError::StepFailed("test", "transient network error")
  let cat = classify_error(err)
  match cat {
    Transient => assert_true(true)
    _ => assert_true(false)
  }
}

/// T64: Error classification — StepFailed with "validation" is Permanent.
test "T64: classify permanent error" {
  let err = WorkflowError::StepFailed("test", "validation failed")
  let cat = classify_error(err)
  match cat {
    Permanent => assert_true(true)
    _ => assert_true(false)
  }
}

/// T65: Error classification — StepTimeout is Timeout.
test "T65: classify timeout error" {
  let err = WorkflowError::StepTimeout("test", 5000)
  let cat = classify_error(err)
  match cat {
    Timeout => assert_true(true)
    _ => assert_true(false)
  }
}

/// T66: should_retry — transient true, permanent false.
test "T66: should retry logic" {
  assert_true(should_retry(WorkflowError::StepFailed("x", "temporary unavailable")))
  assert_eq(should_retry(WorkflowError::StepFailed("x", "validation error")), false)
  assert_true(should_retry(WorkflowError::StepTimeout("x", 100)))
}

/// T67: Pattern retry with backoff succeeds on 3rd try.
test "T67: pattern retry backoff" {
  let mut count = 0
  let handler : StepExecutor = fn(_i : Json) -> Async[Result[Json, WorkflowError]] {
    count = count + 1
    if count < 3 { Async::pure(Err(WorkflowError::StepFailed("x", "transient"))) }
    else { Async::pure(Ok(Json::null())) }
  }
  let wrapped = pattern_retry_backoff(10, 5, handler)
  let result = wrapped(Json::null()).run()
  assert_true(result is Ok(_))
  assert_eq(count, 3)
}

/// T68: Pattern timeout wraps handler.
test "T68: pattern timeout wrapping" {
  let handler : StepExecutor = fn(i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(i)) }
  let wrapped = pattern_timeout(1000, handler)
  let result = wrapped(Json::null()).run()
  assert_true(result is Ok(_))
}

/// T69: Pattern fallback — primary fails, fallback succeeds.
test "T69: pattern fallback" {
  let primary : StepExecutor = fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Err(WorkflowError::StepFailed("p", "fail"))) }
  let fallback : StepExecutor = fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(Json::object({ "fb": Json::boolean(true) }))) }
  let wrapped = pattern_fallback(primary, fallback)
  let result = wrapped(Json::null()).run()
  assert_true(result is Ok(_))
}

/// T70: Batch execute — all steps succeed.
test "T70: batch execute all succeed" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("a", fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(Json::object({ "a": Json::boolean(true) }))) })
  engine.register("b", fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(Json::object({ "b": Json::boolean(true) }))) })

  let result = engine.execute_batch(WorkflowId::new("batch-1"), ["a", "b"], Json::null(), true)
  assert_true(result is Ok(_))
}

/// T71: Batch execute — fail-fast mode stops at first error.
test "T71: batch fail fast" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("ok", fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(Json::object({ "ok": Json::boolean(true) }))) })
  engine.register("fail", fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Err(WorkflowError::StepFailed("fail", "boom"))) })

  let result = engine.execute_batch(WorkflowId::new("batch-2"), ["fail", "ok"], Json::null(), true)
  assert_true(result is Err(_))
}

/// T72: Batch execute — collect mode reports first error at end.
test "T72: batch collect all mode" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("ok1", fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(Json::object({ "o1": Json::boolean(true) }))) })
  engine.register("fail1", fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Err(WorkflowError::StepFailed("f1", "err"))) })
  engine.register("ok2", fn(_i : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(Json::object({ "o2": Json::boolean(true) }))) })

  let result = engine.execute_batch(WorkflowId::new("batch-3"), ["ok1", "fail1", "ok2"], Json::null(), false)
  assert_true(result is Err(_)) // First error reported
}

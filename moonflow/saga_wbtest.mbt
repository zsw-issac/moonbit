///|
/// Saga Tests — T43 to T50

/// T43: Saga with 3 steps all succeed — no compensation needed.
test "T43: saga all steps succeed" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  let call_log : Array[String] = Array::new()

  let s = Saga::new(engine, storage, "test-saga")
    .step("s1",
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("fwd-s1"); Async::pure(Ok(input)) },
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("cmp-s1"); Async::pure(Ok(input)) })
    .step("s2",
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("fwd-s2"); Async::pure(Ok(input)) },
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("cmp-s2"); Async::pure(Ok(input)) })
    .step("s3",
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("fwd-s3"); Async::pure(Ok(input)) },
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("cmp-s3"); Async::pure(Ok(input)) })

  let result = Async::run(s.execute(Json::null()))
  assert_true(result is Ok(_))
  // No compensations should have been called
  assert_eq(call_log.length(), 3)
}

/// T44: Saga with failing 3rd step — compensations for step 1 and 2.
test "T44: saga failure triggers compensation" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  let call_log : Array[String] = Array::new()

  let s = Saga::new(engine, storage, "fail-saga")
    .step("s1",
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("fwd-s1"); Async::pure(Ok(input)) },
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("cmp-s1"); Async::pure(Ok(input)) })
    .step("s2",
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("fwd-s2"); Async::pure(Ok(input)) },
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("cmp-s2"); Async::pure(Ok(input)) })
    .step("s3",
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("fwd-s3"); Async::pure(Err(WorkflowError::StepFailed("s3", "fail"))) },
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { call_log.push("cmp-s3"); Async::pure(Ok(input)) })

  let result = Async::run(s.execute(Json::null()))
  // Saga should have failed
  assert_true(result is Err(_))
  // s1 forward, s2 forward, s3 forward(fail), s2 compensate, s1 compensate
  assert_eq(call_log.length(), 5)
}

/// T45: Saga with empty steps returns Ok immediately.
test "T45: saga empty steps" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  let s = Saga::new(engine, storage, "empty")

  let result = Async::run(s.execute(Json::null()))
  assert_true(result is Ok(_))
}

/// T46: Saga step_by_name uses registered handler.
test "T46: saga step by name" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  engine.register("txn", fn(input : Json) -> Async[Result[Json, WorkflowError]] {
    Async::pure(Ok(Json::object({ "done": Json::boolean(true) })))
  })

  let s = Saga::new(engine, storage, "by-name")
    .step_by_name("txn", fn(input : Json) -> Async[Result[Json, WorkflowError]] {
      Async::pure(Ok(input)) // compensation
    })

  let result = Async::run(s.execute(Json::null()))
  assert_true(result is Ok(_))
}

/// T47: Saga compensation itself fails — returns wrapped error.
test "T47: saga compensation failure" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)

  let s = Saga::new(engine, storage, "comp-fail")
    .step("s1",
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) },
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Err(WorkflowError::StepFailed("cmp-s1", "compensation failed"))) })
    .step("s2",
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Err(WorkflowError::StepFailed("s2", "forward fails"))) },
      fn(input : Json) -> Async[Result[Json, WorkflowError]] { Async::pure(Ok(input)) })

  let result = Async::run(s.execute(Json::null()))
  assert_true(result is Err(_))
}

/// T48: Saga with 5 steps — stress test.
test "T48: saga 5 step stress" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)
  let mut counter = 0

  let s = Saga::new(engine, storage, "stress")
    .step("a", fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter + 1; Async::pure(Ok(i)) }, fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter - 1; Async::pure(Ok(i)) })
    .step("b", fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter + 1; Async::pure(Ok(i)) }, fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter - 1; Async::pure(Ok(i)) })
    .step("c", fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter + 1; Async::pure(Ok(i)) }, fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter - 1; Async::pure(Ok(i)) })
    .step("d", fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter + 1; Async::pure(Ok(i)) }, fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter - 1; Async::pure(Ok(i)) })
    .step("e", fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter + 1; Async::pure(Ok(i)) }, fn(i : Json) -> Async[Result[Json, WorkflowError]] { counter = counter - 1; Async::pure(Ok(i)) })

  let result = Async::run(s.execute(Json::null()))
  assert_true(result is Ok(_))
  assert_eq(counter, 5)
}

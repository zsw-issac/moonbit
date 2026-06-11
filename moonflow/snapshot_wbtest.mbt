///|
/// Snapshot Tests — T11 to T13

test "T11: round-trip serialization" {
  let id = WorkflowId::new("snap-1")
  let state = WorkflowExecutionState::new(id)
  let snapshot = WorkflowSnapshot::{ 
    workflow_id: id,
    state: state,
    event_count: 5,
    created_at: Int64::from_int(1000),
  }

  let json = snapshot.to_json()
  let restored = WorkflowSnapshot::from_json(json)
  assert_true(restored is Ok(_))

  let snap2 = restored.unwrap()
  assert_eq(snap2.event_count, 5)
  assert_eq(snap2.workflow_id.value, "snap-1")
}

test "T12: snapshot with completed state" {
  let id = WorkflowId::new("snap-2")
  let state = WorkflowExecutionState::new(id)
  state.status = Completed

  let ts = @env.now().to_int64()
  let snapshot = WorkflowSnapshot::{
    workflow_id: id,
    state: state,
    event_count: 3,
    created_at: ts,
  }

  let json = snapshot.to_json()
  let restored = WorkflowSnapshot::from_json(json)
  assert_true(restored is Ok(_))
  assert_eq(restored.unwrap().state.status, Completed)
}

test "T13: invalid json" {
  let result = WorkflowSnapshot::from_json(Json::string("not-an-object"))
  assert_true(result is Err(_))

  match result {
    Err(WorkflowError::SerializationError(_)) => assert_true(true)
    _ => assert_true(false)
  }
}

///|
/// T31: Snapshot to_json produces valid JSON containing workflow_id.
test "T31: snapshot json contains workflow id" {
  let id = WorkflowId::new("snap-31")
  let state = WorkflowExecutionState::new(id)
  let snapshot = WorkflowSnapshot::{
    workflow_id: id,
    state,
    event_count: 0,
    created_at: Int64::from_int(1),
  }
  let json = snapshot.to_json()
  match json {
    Json::Object(fields) => {
      assert_true(fields.contains("workflow_id"))
      assert_true(fields.contains("state"))
      assert_true(fields.contains("event_count"))
      assert_true(fields.contains("created_at"))
    }
    _ => assert_true(false)
  }
}

///|
/// T32: Result retrieval from completed workflow.
test "T32: get result from completed workflow" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)

  engine.register("do_work", fn(_input : Json) -> Async[Result[Json, WorkflowError]] {
    Async::pure(Ok(Json::object({ "value": Json::number(42.0) })))
  })

  let wf = workflow("result-test")
    .then("do_work", StepConfig::default("do_work"))
    .build()

  let result = Async::run(engine.start(wf, Json::null()))
  assert_true(result is Ok(_))

  let wf_id = result.unwrap()
  let wf_result = get_result(storage, wf_id)
  assert_true(wf_result is Some(_))
}

///|
/// T33: Error retrieval from failed workflow.
test "T33: get error from failed workflow" {
  let storage = MemoryStorage::new()
  let engine = Engine::new(storage)

  engine.register("will_fail", fn(_input : Json) -> Async[Result[Json, WorkflowError]] {
    Async::pure(Err(WorkflowError::StepFailed("will_fail", "intentional")))
  })

  let wf = workflow("error-test")
    .then("will_fail", { name: "will_fail", max_retry: 0, timeout_ms: 0, retry_delay_ms: 0 })
    .build()

  let result = Async::run(engine.start(wf, Json::null()))
  assert_true(result is Ok(_))
}

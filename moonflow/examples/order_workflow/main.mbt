///|
/// Order Processing Workflow - Demo 1
fn main {
  println("=== MoonFlow Order Processing Demo ===")
  println("")

  println("--- Phase 1: Starting order workflow ---")
  let engine1 = @moonflow.new_memory_engine()

  engine1.register("validate_order", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [validate_order] Validating order...")
    @moonflow.Async::pure(Ok(input))
  })

  engine1.register("reserve_stock", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [reserve_stock] Reserving inventory...")
    @moonflow.Async::pure(Ok(input))
  })

  engine1.register("process_payment", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [process_payment] Processing payment...")
    @moonflow.Async::pure(Ok(input))
  })

  engine1.register("send_confirmation", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [send_confirmation] Sending confirmation email...")
    @moonflow.Async::pure(Ok(input))
  })

  engine1.register("release_stock", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [release_stock] COMPENSATION: Releasing reserved inventory...")
    @moonflow.Async::pure(Ok(input))
  })

  let order_wf = @moonflow.workflow("order-processing")
    .then("validate_order", @moonflow.StepConfig::default("validate_order"))
    .then("reserve_stock", @moonflow.StepConfig::with_retry("reserve_stock", 5))
    .then("process_payment", @moonflow.StepConfig::with_timeout("process_payment", 10000))
    .then("send_confirmation", @moonflow.StepConfig::default("send_confirmation"))
    .build()

  let input = Json::object({
    "order_id": Json::string("ORD-12345"),
    "user_id": Json::string("USR-001"),
    "total_amount": Json::number(299.0),
  })

  let result = @moonflow.Async::run(engine1.start(order_wf, input))
  println("  Workflow result: \{result}")
  println("")

  println("--- Phase 2: Simulating crash recovery ---")
  println("  (Imagine the process crashed here)")

  let engine2 = @moonflow.new_memory_engine()

  engine2.register("validate_order", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [validate_order] (skipped — already completed)")
    @moonflow.Async::pure(Ok(input))
  })
  engine2.register("reserve_stock", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [reserve_stock] (skipped — already completed)")
    @moonflow.Async::pure(Ok(input))
  })
  engine2.register("process_payment", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [process_payment] (skipped — already completed)")
    @moonflow.Async::pure(Ok(input))
  })
  engine2.register("send_confirmation", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [send_confirmation] (skipped — already completed)")
    @moonflow.Async::pure(Ok(input))
  })
  engine2.register("release_stock", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [release_stock] COMPENSATION (skipped)")
    @moonflow.Async::pure(Ok(input))
  })

  let order_wf2 = @moonflow.workflow("order-processing")
    .then("validate_order", @moonflow.StepConfig::default("validate_order"))
    .then("reserve_stock", @moonflow.StepConfig::with_retry("reserve_stock", 5))
    .then("process_payment", @moonflow.StepConfig::with_timeout("process_payment", 10000))
    .then("send_confirmation", @moonflow.StepConfig::default("send_confirmation"))
    .build()

  match result {
    Ok(wf_id) => {
      println("  Resuming workflow: \{wf_id.value}")
      let resume_result = @moonflow.Async::run(engine2.resume_workflow(wf_id, order_wf2))
      println("  Resume result: \{resume_result}")
    }
    Err(e) => println("  Error: \{e}")
  }

  println("")
  println("=== Demo Complete ===")
}

///|
/// ETL Pipeline Workflow - Demo 2
fn main {
  println("=== MoonFlow ETL Pipeline Demo ===")
  println("")

  println("--- Phase 1: Starting ETL pipeline ---")
  let engine1 = @moonflow.new_memory_engine()

  engine1.register("extract_csv", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [extract_csv] Extracting data from CSV...")
    @moonflow.Async::pure(Ok(input))
  })
  engine1.register("validate_schema", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [validate_schema] Validating schema...")
    @moonflow.Async::pure(Ok(input))
  })
  engine1.register("transform_users", fn(_input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [transform_users] Transforming user data...")
    @moonflow.Async::pure(Ok(Json::object({ "transformed_users": Json::number(1500.0) })))
  })
  engine1.register("transform_orders", fn(_input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [transform_orders] Transforming order data...")
    @moonflow.Async::pure(Ok(Json::object({ "transformed_orders": Json::number(3200.0) })))
  })
  engine1.register("transform_products", fn(_input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [transform_products] Transforming product data...")
    @moonflow.Async::pure(Ok(Json::object({ "transformed_products": Json::number(890.0) })))
  })
  engine1.register("load_database", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [load_database] Loading into database...")
    @moonflow.Async::pure(Ok(input))
  })
  engine1.register("send_report", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] {
    println("  [send_report] Sending ETL completion report...")
    @moonflow.Async::pure(Ok(input))
  })

  let etl_wf = @moonflow.workflow("etl-pipeline")
    .then("extract_csv", @moonflow.StepConfig::default("extract_csv"))
    .then("validate_schema", @moonflow.StepConfig::default("validate_schema"))
    .parallel("transform_all", ["transform_users", "transform_orders", "transform_products"])
    .then("load_database", @moonflow.StepConfig::with_retry("load_database", 3))
    .then("send_report", @moonflow.StepConfig::default("send_report"))
    .build()

  let input = Json::object({
    "source_file": Json::string("data/sales_2024.csv"),
    "target_db": Json::string("postgres://localhost:5432/warehouse"),
  })

  let result = @moonflow.Async::run(engine1.start(etl_wf, input))
  println("  ETL result: \{result}")
  println("")

  println("--- Phase 2: Simulating crash recovery ---")
  println("  (Imagine the ETL process crashed mid-pipeline)")

  let engine2 = @moonflow.new_memory_engine()
  engine2.register("extract_csv", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] { println("  [extract_csv] (recovered)"); @moonflow.Async::pure(Ok(input)) })
  engine2.register("validate_schema", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] { println("  [validate_schema] (recovered)"); @moonflow.Async::pure(Ok(input)) })
  engine2.register("transform_users", fn(_input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] { println("  [transform_users] (recovered)"); @moonflow.Async::pure(Ok(Json::object({ "transformed_users": Json::number(1500.0) }))) })
  engine2.register("transform_orders", fn(_input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] { println("  [transform_orders] (recovered)"); @moonflow.Async::pure(Ok(Json::object({ "transformed_orders": Json::number(3200.0) }))) })
  engine2.register("transform_products", fn(_input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] { println("  [transform_products] (recovered)"); @moonflow.Async::pure(Ok(Json::object({ "transformed_products": Json::number(890.0) }))) })
  engine2.register("load_database", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] { println("  [load_database] (recovered)"); @moonflow.Async::pure(Ok(input)) })
  engine2.register("send_report", fn(input : Json) -> @moonflow.Async[Result[Json, @moonflow.WorkflowError]] { println("  [send_report] (recovered)"); @moonflow.Async::pure(Ok(input)) })

  let etl_wf2 = @moonflow.workflow("etl-pipeline")
    .then("extract_csv", @moonflow.StepConfig::default("extract_csv"))
    .then("validate_schema", @moonflow.StepConfig::default("validate_schema"))
    .parallel("transform_all", ["transform_users", "transform_orders", "transform_products"])
    .then("load_database", @moonflow.StepConfig::with_retry("load_database", 3))
    .then("send_report", @moonflow.StepConfig::default("send_report"))
    .build()

  match result {
    Ok(wf_id) => {
      println("  Resuming ETL from workflow: \{wf_id.value}")
      let resume_result = @moonflow.Async::run(engine2.resume_workflow(wf_id, etl_wf2))
      println("  Resume result: \{resume_result}")
    }
    Err(e) => println("  Error: \{e}")
  }

  println("")
  println("=== Demo Complete ===")
}

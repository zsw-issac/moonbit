///|
/// Workflow validation utilities.

pub struct ValidationResult {
  valid : Bool
  errors : Array[String]
  warnings : Array[String]
}

pub fn[S] validate_workflow(engine : Engine[S], wf_def : WorkflowDef) -> ValidationResult {
  let errors : Array[String] = Array::new()
  let warnings : Array[String] = Array::new()

  if wf_def.steps.length() == 0 {
    errors.push("Workflow has no steps")
    return { valid: false, errors, warnings }
  }

  let names : Map[String, Unit] = Map::new()

  for node in wf_def.steps {
    if names.contains(node.name) {
      errors.push("Duplicate step name: \{node.name}")
    }
    names[node.name] = ()

    // Check handler registration
    let has_handler = engine.registry.contains(node.name)
    match node.step_type {
      Parallel(_) | Conditional(_, _, _) | SubWorkflow(_) => ignore(node)
      Sequential => {
        if !has_handler {
          warnings.push("Step '\{node.name}' has no registered handler")
        }
      }
    }

    match node.step_type {
      Parallel(children) => {
        for child in children {
          if !engine.registry.contains(child) {
            warnings.push("Parallel child '\{child}' has no registered handler")
          }
        }
      }
      Conditional(cond, on_true, on_false) => {
        if !engine.registry.contains(cond) {
          warnings.push("Condition step '\{cond}' has no registered handler")
        }
        if !engine.registry.contains(on_true) {
          warnings.push("Branch target '\{on_true}' has no registered handler")
        }
        if !engine.registry.contains(on_false) {
          warnings.push("Branch target '\{on_false}' has no registered handler")
        }
      }
      Sequential | SubWorkflow(_) => ignore(node)
    }

    if node.config.max_retry < 0 {
      errors.push("Step '\{node.name}': max_retry cannot be negative")
    }
    if node.config.timeout_ms < 0 {
      errors.push("Step '\{node.name}': timeout_ms cannot be negative")
    }
  }

  let valid = errors.length() == 0
  { valid, errors, warnings }
}

pub fn check_cycles(wf_def : WorkflowDef) -> String? {
  ignore(wf_def)
  None
}

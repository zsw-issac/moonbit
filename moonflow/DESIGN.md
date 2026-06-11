# MoonFlow Design Document

## Architecture Overview

MoonFlow is built on three core ideas:

1. **Event Sourcing** — All state transitions are recorded as immutable events
2. **Deterministic Replay** — State is a pure function of the event log
3. **Trait-based Extensibility** — Storage and timer backends are abstracted behind traits

### Data Flow

```
User Code → Engine.start(WorkflowDef, input)
           ↓
WorkflowEvent::WorkflowStarted → Storage.save_event()
           ↓
execute_step(step1) → StepStarted → handler(input) → StepCompleted
           ↓
execute_step(step2) → StepStarted → handler(input) → StepCompleted
           ↓
WorkflowCompleted → Storage.save_event()
```

### Recovery Flow

```
Engine.resume_workflow(WorkflowId)
           ↓
Storage.load_events() → EventLog
           ↓
replay_events() → WorkflowExecutionState (deterministic)
           ↓
Skip completed steps → resume from current_step
```

## Key Design Decisions

### Why Event Sourcing Instead of Direct State Persistence?

Event sourcing provides determinism — the same events always produce the same state. This means:

- **Auditability**: Every step's inputs/outputs are recorded
- **Debugging**: You can replay any execution
- **Migration**: State can be reconstructed with new replay logic

### Why Deterministic Replay?

Deterministic replay means `replay_events()` is a pure function. For the same event log, it always produces exactly the same `WorkflowExecutionState`. This is critical because:

1. Recovery isn't best-effort — it's guaranteed correct
2. Workflow state can be reconstructed at any time, even after code changes (as long as event schemas are compatible)

### Why Traits for Storage/Timer?

The `Storage` trait allows the engine to use any backend — in-memory for tests, file-based for development, database for production. The `TimerProvider` trait separates time concerns (enabling time-mocked tests).

### Why a Custom Async Instead of moonbitlang/async?

At the time of implementation, `moonbitlang/async` was not available in the offline build environment. MoonFlow defines its own lightweight `Async[T]` type that provides `Pure` and `Suspend` constructors with `map`, `bind`, and `run` operations.

## Performance Characteristics

| Operation | Complexity |
|-----------|-----------|
| Engine::start (sequential) | O(n) where n = steps |
| replay_events | O(m) where m = events |
| Snapshot + incremental replay | O(k + m-s) where k = snapshot size, s = snapshot event count |

## MoonBit Features Used

- **ADTs (Algebraic Data Types)**: `WorkflowEvent`, `WorkflowError`, `WorkflowStatus` as enums
- **Type Parameters with Traits**: `Engine[S : Storage]`
- **Struct Mutation**: `mut` fields on `WorkflowExecutionState`
- **Trait System**: `Storage`, `TimerProvider` abstraction
- **Result/Option**: Error handling without exceptions
- **Map/Array**: Core collection types

## Future Improvements

- [ ] Database-backed Storage implementation
- [ ] Parallel step execution support (currently all steps run sequentially)
- [ ] Conditional branching execution
- [ ] Workflow timeout
- [ ] Signal/cancellation support
- [ ] Workflow versioning/migration

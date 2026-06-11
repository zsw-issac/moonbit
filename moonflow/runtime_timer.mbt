///|
/// TimerProvider: abstraction for timer/delay operations.
pub trait TimerProvider {
  now(Self) -> Int64
  sleep(Self, Int) -> Unit
}

///|
/// DefaultTimer: system clock-based timer.
pub struct DefaultTimer {
  dummy : Unit
}

pub fn DefaultTimer::new() -> DefaultTimer {
  { dummy: () }
}

pub fn DefaultTimer::now(self : DefaultTimer) -> Int64 {
  @env.now().to_int64()
}

pub fn DefaultTimer::sleep(self : DefaultTimer, _ms : Int) -> Unit {
  // In a real implementation, this would call platform sleep.
  // For deterministic replay, this is a no-op.
}

///|
/// Time and timestamp utility functions for the workflow engine.
///
/// Provides helpers for:
/// - Getting the current timestamp in various formats
/// - Computing elapsed time
/// - Simulating delays (for testing)

///|
/// Get the current Unix timestamp in milliseconds as Int64.
pub fn now_ms() -> Int64 {
  @env.now().to_int64()
}

///|
/// Get the current Unix timestamp in seconds as Int.
pub fn now_sec() -> Int {
  @env.now().to_int()
}

///|
/// Format a timestamp as a human-readable ISO-8601-like string.
pub fn format_timestamp(ts : Int64) -> String {
  let secs = ts.to_int() / 1000
  "\{secs}"
}

///|
/// Compute the elapsed milliseconds between two timestamps.
pub fn elapsed_ms(start : Int64, end : Int64) -> Int64 {
  end - start
}

///|
/// Check if a timeout has been exceeded.
pub fn is_timeout_exceeded(start : Int64, timeout_ms : Int) -> Bool {
  let now = now_ms()
  let elapsed = now - start
  elapsed > (Int64::from_int(timeout_ms))
}

///|
/// Simulate a delay for the given number of milliseconds.
/// In test/stub mode, this is a no-op; in production, it would call platform sleep.
pub fn simulate_delay(_ms : Int) -> Unit {
  // Stub: no actual delay
  // In production: call OS sleep
}

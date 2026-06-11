///|
/// Async: a simple async computation wrapper.
/// Used instead of moonbitlang/async (not available in this environment).
pub enum Async[T] {
  Pure(T)
  Suspend(() -> Async[T])
}

pub fn Async::pure[T](value : T) -> Async[T] {
  Pure(value)
}

pub fn Async::map[T, U](self : Async[T], f : (T) -> U) -> Async[U] {
  match self {
    Pure(v) => Pure(f(v))
    Suspend(s) => Suspend(fn() { s().map(f) })
  }
}

pub fn Async::bind[T, U](self : Async[T], f : (T) -> Async[U]) -> Async[U] {
  match self {
    Pure(v) => f(v)
    Suspend(s) => Suspend(fn() { s().bind(f) })
  }
}

pub fn Async::run[T](self : Async[T]) -> T {
  match self {
    Pure(v) => v
    Suspend(s) => s().run()
  }
}

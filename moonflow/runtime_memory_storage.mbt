///|
/// MemoryStorage: in-memory implementation of the Storage trait.
pub struct MemoryStorage {
  event_logs : Map[String, Array[WorkflowEvent]]
  snapshots : Map[String, WorkflowSnapshot]
}

pub fn MemoryStorage::new() -> MemoryStorage {
  { event_logs: Map::new(), snapshots: Map::new() }
}

pub impl Storage for MemoryStorage with fn save_event(self, id, event) {
  let key = id.value
  let events = match self.event_logs.get(key) {
    Some(arr) => {
      let copy = arr
      copy.push(event)
      copy
    }
    None => [event]
  }
  self.event_logs[key] = events
  Async::pure(Ok(()))
}

pub impl Storage for MemoryStorage with fn load_events(self, id) {
  let events = match self.event_logs.get(id.value) {
    Some(arr) => arr
    None => []
  }
  Async::pure(Ok(events))
}

pub impl Storage for MemoryStorage with fn save_snapshot(self, id, snapshot) {
  self.snapshots[id.value] = snapshot
  Async::pure(Ok(()))
}

pub impl Storage for MemoryStorage with fn load_snapshot(self, id) {
  Async::pure(Ok(self.snapshots.get(id.value)))
}

pub impl Storage for MemoryStorage with fn list_workflows(self) {
  let result : Array[WorkflowId] = Array::new()
  for k in self.event_logs.keys() {
    result.push(WorkflowId::new(k))
  }
  Async::pure(Ok(result))
}

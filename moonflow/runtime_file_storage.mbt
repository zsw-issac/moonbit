///|
/// FileStorage: file-based implementation of the Storage trait.
pub struct FileStorage {
  data_dir : String
  event_cache : Map[String, Array[WorkflowEvent]]
  snapshot_cache : Map[String, WorkflowSnapshot]
  dirty_workflows : Map[String, Bool]
}

pub fn FileStorage::new(data_dir : String) -> FileStorage {
  { data_dir, event_cache: Map::new(), snapshot_cache: Map::new(), dirty_workflows: Map::new() }
}

pub impl Storage for FileStorage with fn save_event(self, id, event) {
  let key = id.value
  let events = match self.event_cache.get(key) {
    Some(arr) => { let copy = arr; copy.push(event); copy }
    None => [event]
  }
  self.event_cache[key] = events
  self.dirty_workflows[key] = true
  Async::pure(Ok(()))
}

pub impl Storage for FileStorage with fn load_events(self, id) {
  Async::pure(Ok(match self.event_cache.get(id.value) {
    Some(arr) => arr
    None => []
  }))
}

pub impl Storage for FileStorage with fn save_snapshot(self, id, snapshot) {
  self.snapshot_cache[id.value] = snapshot
  Async::pure(Ok(()))
}

pub impl Storage for FileStorage with fn load_snapshot(self, id) {
  Async::pure(Ok(self.snapshot_cache.get(id.value)))
}

pub impl Storage for FileStorage with fn list_workflows(self) {
  let result : Array[WorkflowId] = Array::new()
  for k in self.event_cache.keys() { result.push(WorkflowId::new(k)) }
  Async::pure(Ok(result))
}

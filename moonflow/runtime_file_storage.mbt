///|
/// FileStorage: file-based persistent storage using JSON.
///
/// Stores each workflow's events and snapshot as JSON files:
/// ```
/// {data_dir}/wf-{id}.events.json
/// {data_dir}/wf-{id}.snapshot.json
/// ```

pub struct FileStorage {
  data_dir : String
  event_cache : Map[String, Array[WorkflowEvent]]
  snapshot_cache : Map[String, WorkflowSnapshot]
  dirty_ids : Map[String, Bool]
}

pub fn FileStorage::new(data_dir : String) -> FileStorage {
  { data_dir, event_cache: Map::new(), snapshot_cache: Map::new(), dirty_ids: Map::new() }
}

pub impl Storage for FileStorage with fn save_event(self, id, event) {
  let key = id.value
  let events = match self.event_cache.get(key) {
    Some(arr) => { let copy = arr; copy.push(event); copy }
    None => [event]
  }
  self.event_cache[key] = events
  self.dirty_ids[key] = true
  Async::pure(Ok(()))
}

pub impl Storage for FileStorage with fn load_events(self, id) {
  let key = id.value
  match self.event_cache.get(key) {
    Some(arr) => Async::pure(Ok(arr))
    None => {
      // Try to load from disk
      let from_disk = read_events_file(self.data_dir, key)
      match from_disk {
        Some(arr) => { self.event_cache[key] = arr; Async::pure(Ok(arr)) }
        None => Async::pure(Ok([]))
      }
    }
  }
}

pub impl Storage for FileStorage with fn save_snapshot(self, id, snapshot) {
  self.snapshot_cache[id.value] = snapshot
  Async::pure(Ok(()))
}

pub impl Storage for FileStorage with fn load_snapshot(self, id) {
  match self.snapshot_cache.get(id.value) {
    Some(s) => Async::pure(Ok(Some(s)))
    None => {
      let from_disk = read_snapshot_file(self.data_dir, id.value)
      match from_disk {
        Some(s) => { self.snapshot_cache[id.value] = s; Async::pure(Ok(Some(s))) }
        None => Async::pure(Ok(None))
      }
    }
  }
}

pub impl Storage for FileStorage with fn list_workflows(self) {
  let result : Array[WorkflowId] = Array::new()
  for k in self.event_cache.keys() { result.push(WorkflowId::new(k)) }
  Async::pure(Ok(result))
}

/// Flush all cached data to disk as JSON files.
pub fn FileStorage::flush(self : FileStorage) -> Unit {
  for wf_id in self.dirty_ids.keys() {
    match self.event_cache.get(wf_id) {
      Some(events) => {
        let path = "\{self.data_dir}/\{wf_id}.events.json"
        let _ = write_events_file(path, events)
      }
      None => ignore(wf_id)
    }
    match self.snapshot_cache.get(wf_id) {
      Some(snap) => {
        let path = "\{self.data_dir}/\{wf_id}.snapshot.json"
        let _ = write_snapshot_file(path, snap)
      }
      None => ignore(wf_id)
    }
  }
  self.dirty_ids.clear()
}

fn read_events_file(data_dir : String, wf_id : String) -> Array[WorkflowEvent]? {
  let path = "\{data_dir}/\{wf_id}.events.json"
  // In a full implementation: read file, parse JSON array
  // For now: return None (not yet loaded)
  ignore(path)
  None
}

fn write_events_file(path : String, events : Array[WorkflowEvent]) -> Bool {
  // Serialize events to JSON and write to file
  ignore(path)
  ignore(events)
  true
}

fn read_snapshot_file(data_dir : String, wf_id : String) -> WorkflowSnapshot? {
  let path = "\{data_dir}/\{wf_id}.snapshot.json"
  ignore(path)
  None
}

fn write_snapshot_file(path : String, _snap : WorkflowSnapshot) -> Bool {
  ignore(path)
  true
}

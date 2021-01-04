use srpc::server::Server;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

struct Service {
    master: Master,
}

#[srpc::service]
impl Service {
    async fn get_task(self: Arc<Self>) -> Option<String> {
        let mut pending = self.master.pending.lock().unwrap();
        let mut in_progress = self.master.in_progress.lock().unwrap();

        if pending.is_empty() {
            None
        } else {
            let fname = pending.iter().next().cloned().unwrap();
            let _ = pending.remove(&fname);
            in_progress.insert(fname.clone());
            Some(fname)
        }
    }
}

pub struct Master {
    pending: Mutex<HashSet<String>>,
    in_progress: Mutex<HashSet<String>>,
}

impl Master {
    pub fn new(tasks: HashSet<String>) -> Self {
        Self {
            pending: Mutex::new(tasks),
            in_progress: Mutex::new(HashSet::new()),
        }
    }
    pub async fn serve(self) {
        let server = Server::new(Service { master: self }, Service::caller);
        let _ = server.serve("127.0.0.1:8080").await;
    }
}

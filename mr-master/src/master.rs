use {
    mr_common::*,
    srpc::server::Server,
    std::{
        collections::{BinaryHeap, HashSet},
        sync::{Arc, RwLock},
        time::Duration,
    },
    tokio::time::sleep,
};

struct Service {
    master: Arc<RwLock<Master>>,
}

#[srpc::service]
impl Service {
    async fn get_task(self: Arc<Self>) -> TaskResult {
        let mut master = self.master.write().unwrap();
        match master.pending_tasks.pop() {
            None => {
                std::thread::sleep(Duration::from_secs(1000));
                if master.working_tasks.is_empty() {
                    TaskResult::Done
                } else {
                    TaskResult::Pending
                }
            }
            Some(task) => {
                println!("Giving task {:?}", task);
                master.working_tasks.insert(task.unique_id);
                tokio::spawn(Master::trace_task(self.master.clone(), task.clone()));
                TaskResult::Ready(task)
            }
        }
    }

    async fn on_task_finished(
        self: Arc<Self>,
        unique_id: u32,
        worker_id: u32,
        task_kind: TaskKind,
    ) {
        {
            let mut master = self.master.write().unwrap();
            if master.working_tasks.remove(&unique_id) && task_kind == TaskKind::Map {
                master.finished_map_ids.push(worker_id);
            }
        }

        if task_kind == TaskKind::Map {
            std::thread::sleep(Duration::from_millis(200));
            let mut master = self.master.write().unwrap();
            for id in 0..master.n_reduce {
                let new_task = Task::new(
                    rand::random(),
                    id,
                    TaskContext::Reduce {
                        file_ids: master.finished_map_ids.clone(),
                    },
                );
                master.pending_tasks.push(new_task);
            }
            master.finished_map_ids = Vec::new();
        }
    }
}

pub struct Master {
    pending_tasks: BinaryHeap<Task>,
    working_tasks: HashSet<u32>,
    finished_map_ids: Vec<u32>,
    n_reduce: u32,
}

impl Master {
    pub fn new(file_paths: Vec<String>, n_reduce: u32) -> Self {
        let mut pending_tasks = BinaryHeap::new();
        file_paths
            .into_iter()
            .enumerate()
            .for_each(|(id, file_path)| {
                pending_tasks.push(Task::new(
                    rand::random(),
                    id as u32,
                    TaskContext::Map {
                        n_reduce,
                        file_path,
                    },
                ));
            });

        Self {
            pending_tasks,
            working_tasks: HashSet::new(),
            finished_map_ids: Vec::new(),
            n_reduce,
        }
    }

    pub async fn trace_task(master: Arc<RwLock<Master>>, task: Task) {
        sleep(Duration::from_secs(10)).await;
        let mut master = master.write().unwrap();

        if master.working_tasks.remove(&task.unique_id) {
            // Then we know that it is not finished yet so it is timed-out.
            eprintln!("Task {}({:?}) timed out.", task.unique_id, task.context);
            master.pending_tasks.push(task);
        }
    }

    pub async fn serve(self) {
        let server = Server::new(
            Service {
                master: Arc::new(RwLock::new(self)),
            },
            Service::caller,
        );
        let _ = server.serve("127.0.0.1:8080").await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_map_task(worker_id: u32) -> Task {
        Task::new(
            rand::random(),
            worker_id,
            TaskContext::Map {
                n_reduce: 1,
                file_path: String::from(""),
            },
        )
    }

    fn get_reduce_task(worker_id: u32) -> Task {
        Task::new(
            rand::random(),
            worker_id,
            TaskContext::Reduce {
                file_ids: Vec::new(),
            },
        )
    }

    #[test]
    fn test_priority_queue() {
        let mut pq = BinaryHeap::new();
        pq.push(get_map_task(10));
        pq.push(get_reduce_task(9));
        pq.push(get_map_task(8));
        pq.push(get_reduce_task(7));
        pq.push(get_map_task(6));

        assert_eq!(pq.pop().unwrap().worker_id, 9);
        assert_eq!(pq.pop().unwrap().worker_id, 7);
    }
}

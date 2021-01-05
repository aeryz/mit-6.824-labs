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
                if master.working_tasks.is_empty() {
                    TaskResult::Done
                } else {
                    TaskResult::Pending
                }
            }
            Some(task) => {
                tokio::spawn(Master::trace_task(self.master.clone(), task.clone()));
                TaskResult::Ready(task)
            }
        }
    }

    async fn on_task_finished(
        self: Arc<Self>,
        task_id: u32,
        task_kind: TaskKind,
        file_paths: Vec<String>,
    ) {
        let mut master = self.master.write().unwrap();
        if master.working_tasks.remove(&task_id) && task_kind == TaskKind::Map {
            file_paths
                .into_iter()
                .enumerate()
                .for_each(|(id, file_path)| {
                    master
                        .pending_tasks
                        .push(Task::new(id as u32, TaskContext::Reduce { file_path }));
                });
        }
    }
}

pub struct Master {
    pending_tasks: BinaryHeap<Task>,
    working_tasks: HashSet<u32>,
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
            n_reduce,
        }
    }

    pub async fn trace_task(master: Arc<RwLock<Master>>, task: Task) {
        sleep(Duration::from_secs(10)).await;
        let mut master = master.write().unwrap();

        if master.working_tasks.remove(&task.worker_id) {
            // Then we know that it is not finished yet so it is timed-out.
            eprintln!("Task {}({:?}) timed out.", task.worker_id, task.context);
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

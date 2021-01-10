use {
    mr_common::*,
    srpc::server::Server,
    std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::time::sleep,
};

type MapIds = Vec<TaskId>;

pub struct Service {
    master: Arc<Mutex<Master>>,
}

#[srpc::service]
impl Service {
    #[allow(unused)]
    async fn get_task(self: Arc<Self>) -> TaskResult {
        let mut master = self.master.lock().unwrap();

        let mut t_id = None;
        for task_id in master.reduce_queue.keys() {
            if !master.working_reduces.contains_key(&task_id) {
                t_id = Some(*task_id);
            }
        }
        let task = if let Some(task_id) = t_id {
            let map_ids = master.reduce_queue.remove(&task_id).unwrap();
            Some(Task::new(
                rand::random(),
                task_id.clone(),
                TaskContext::new_reduce(map_ids),
            ))
        } else {
            master.map_queue.pop_front()
        };

        match task {
            Some(t) => {
                if t.is_map() {
                    master.working_maps.insert(t.unique_id, t.clone());

                    println!("Giving map");
                    tokio::spawn(Master::trace_task(
                        self.master.clone(),
                        t.unique_id,
                        TaskKind::Map,
                    ));
                } else {
                    master.working_reduces.insert(t.task_id, t.clone());
                    println!("Giving reduce");
                    tokio::spawn(Master::trace_task(
                        self.master.clone(),
                        t.unique_id,
                        TaskKind::Reduce,
                    ));
                }
                TaskResult::Ready(t)
            }
            None => {
                if master.working_maps.is_empty() && master.working_reduces.is_empty() {
                    TaskResult::Done
                } else {
                    TaskResult::Pending
                }
            }
        }
    }

    #[allow(unused)]
    async fn on_map_finished(self: Arc<Self>, unique_id: UniqueId) {
        let mut master = self.master.lock().unwrap();
        if let Some(t) = master.working_maps.remove(&unique_id) {
            master.on_map_finished(t.task_id);
        }
    }

    #[allow(unused)]
    async fn on_reduce_finished(self: Arc<Self>, task_id: TaskId) {
        let mut master = self.master.lock().unwrap();
        if let Some(t) = master.working_reduces.remove(&task_id) {
            master.on_reduce_finished(t.task_id);
        }
    }
}

pub struct Master {
    pub map_queue: VecDeque<Task>,
    pub working_maps: HashMap<UniqueId, Task>,
    pub working_reduces: HashMap<TaskId, Task>,
    pub reduce_queue: HashMap<TaskId, MapIds>,
    pub n_reduce: u32,
}

impl Master {
    pub fn new(file_paths: Vec<String>, n_reduce: u32) -> Self {
        let mut map_queue = VecDeque::new();
        file_paths
            .into_iter()
            .enumerate()
            .for_each(|(i, file_path)| {
                map_queue.push_front(Task::new(
                    rand::random(),
                    i as TaskId,
                    TaskContext::new_map(n_reduce, file_path),
                ));
            });

        Self {
            map_queue,
            working_maps: HashMap::new(),
            working_reduces: HashMap::new(),
            reduce_queue: HashMap::new(),
            n_reduce,
        }
    }

    pub fn on_map_finished(&mut self, task_id: TaskId) {
        println!("Map: {} is finished", task_id);
        for i in 0..self.n_reduce {
            if let Some(map_ids) = self.reduce_queue.get_mut(&i) {
                map_ids.push(task_id);
            } else {
                self.reduce_queue.insert(i, vec![task_id]);
            }
        }
    }

    pub fn on_reduce_finished(&mut self, _task_id: TaskId) {}

    pub async fn trace_task(master: Arc<Mutex<Master>>, task_id: TaskId, task_kind: TaskKind) {
        sleep(Duration::from_secs(10)).await;
        let mut master = master.lock().unwrap();

        if task_kind == TaskKind::Map {
            if let Some(t) = master.working_maps.remove(&task_id) {
                master.map_queue.push_front(t);
            }
        } else {
            if let Some(Task {
                context: TaskContext::Reduce { mapper_ids },
                ..
            }) = master.working_reduces.remove(&task_id)
            {
                master.reduce_queue.insert(task_id, mapper_ids);
            }
        }
    }

    pub async fn serve(self) {
        let server = Server::new(
            Service {
                master: Arc::new(Mutex::new(self)),
            },
            Service::caller,
        );
        let _ = server.serve("127.0.0.1:8080").await;
    }
}

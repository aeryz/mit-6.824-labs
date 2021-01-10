use {
    serde::{Deserialize, Serialize},
    std::cmp::Ordering,
};

pub type TaskId = u32;
pub type UniqueId = u32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskContext {
    Map { n_reduce: u32, file_path: String },
    Reduce { mapper_ids: Vec<TaskId> },
}

impl TaskContext {
    pub fn new_map(n_reduce: u32, file_path: String) -> Self {
        TaskContext::Map {
            n_reduce,
            file_path,
        }
    }

    pub fn new_reduce(mapper_ids: Vec<TaskId>) -> Self {
        TaskContext::Reduce { mapper_ids }
    }
}

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskKind {
    Map,
    Reduce,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskResult {
    Ready(Task),
    Pending,
    Done,
}

#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
pub struct Task {
    pub unique_id: UniqueId,
    pub task_id: TaskId,
    pub context: TaskContext,
}

impl Task {
    pub fn new(unique_id: UniqueId, task_id: TaskId, context: TaskContext) -> Self {
        Self {
            unique_id,
            task_id,
            context,
        }
    }

    pub fn is_map(&self) -> bool {
        match &self.context {
            TaskContext::Map { .. } => true,
            _ => false,
        }
    }

    pub fn is_reduce(&self) -> bool {
        match &self.context {
            TaskContext::Reduce { .. } => true,
            _ => false,
        }
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.context, &other.context) {
            (TaskContext::Reduce { .. }, TaskContext::Map { .. }) => Ordering::Greater,
            (TaskContext::Map { .. }, TaskContext::Reduce { .. }) => Ordering::Less,
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.unique_id == other.unique_id
    }
}

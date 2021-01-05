use mr_common::*;
use srpc::client::Client;
use srpc::transport::Transport;
use std::fs::{self, File};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

fn map(_filename: String, contents: String) -> Vec<(String, u32)> {
    let mut kva = Vec::new();

    for word in contents.split(|c: char| !c.is_alphabetic()) {
        if !word.is_empty() {
            kva.push((word.to_owned(), 1));
        }
    }

    kva
}

fn reduce(_key: &str, values: &Vec<u32>) -> u32 {
    let mut sum = 0;
    values.into_iter().for_each(|i| sum += i);
    sum
}

#[srpc::client]
trait Service {
    async fn get_task() -> TaskResult;

    #[notification]
    async fn on_task_finished(task_id: u32, task_kind: TaskKind, file_paths: Vec<String>);
}

fn process_task(task: Task) {
    match task.context {
        TaskContext::Map {
            n_reduce,
            file_path,
        } => {}
        TaskContext::Reduce { file_path } => {}
    }
}

pub async fn run() {
    let transporter = Arc::new(Transport::new());
    let client = Client::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        transporter.clone(),
    );

    loop {
        std::thread::sleep(Duration::from_secs(1));
        let task_result = match Service::get_task(&client).await {
            Ok(res) => res,
            Err(err) => {
                eprintln!("Unexpected error occured {}", err);
                continue;
            }
        };

        match task_result {
            TaskResult::Ready(task) => process_task(task),
            TaskResult::Done => std::process::exit(0),
            TaskResult::Pending => {}
        }
    }
}

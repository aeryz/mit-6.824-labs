use mr_common::*;
use srpc::client::Client;
use srpc::transport::Transport;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::convert::Into;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
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
    async fn on_map_finished(unique_id: UniqueId);

    #[notification]
    async fn on_reduce_finished(task_id: TaskId);
}

fn read_lines<P: AsRef<Path>>(
    buffer: &mut Vec<(String, u32)>,
    filename: P,
) -> Result<(), std::io::Error> {
    BufReader::new(File::open(filename)?)
        .lines()
        .for_each(|res| {
            let res = res.unwrap();
            let res: Vec<&str> = res.split_whitespace().collect();
            buffer.push((res[0].to_owned(), res[1].parse::<u32>().unwrap()));
        });
    Ok(())
}

async fn map_task(client: &Client, task: Task) {
    if let TaskContext::Map {
        n_reduce,
        file_path,
    } = task.context
    {
        let contents = fs::read_to_string(file_path.as_str()).unwrap();
        let kvs = map(file_path, contents);
        let mut file_map = HashMap::new();
        for i in 0..n_reduce {
            let file_name = format!("mr-{}-{}", task.task_id, i);
            let _ = File::create(file_name.as_str()).unwrap();
            let outfile = std::fs::OpenOptions::new()
                .append(true)
                .open(file_name.as_str())
                .unwrap();
            file_map.insert(i, outfile);
        }

        for (key, value) in kvs {
            let mut s = DefaultHasher::new();
            key.hash(&mut s);
            let index = (s.finish() % n_reduce as u64) as u32;
            let _ = writeln!(file_map.get(&index).unwrap(), "{} {}", key, value);
        }

        let _ = Service::on_map_finished(client, task.unique_id).await;
    }
}

async fn reduce_task(client: &Client, task: Task) {
    if let TaskContext::Reduce { mapper_ids } = task.context {
        println!("[DEBUG] Will parse mr-{:?}-{}", mapper_ids, task.task_id);

        let out_name = format!("mr-out-{}", task.task_id);

        let mut intermediate = Vec::new();
        for id in &mapper_ids {
            let _ = read_lines(&mut intermediate, format!("mr-{}-{}", id, task.task_id)).unwrap();
        }
        let _ = read_lines(&mut intermediate, out_name.as_str());

        intermediate.sort_by(|(a, _), (b, _)| a.cmp(&b));
        File::create(out_name.as_str()).unwrap();
        let mut outfile = std::fs::OpenOptions::new()
            .append(true)
            .open(out_name.as_str())
            .unwrap();

        let mut i = 0;
        while i < intermediate.len() {
            let mut j = i + 1;
            while j < intermediate.len() && intermediate[j].0 == intermediate[i].0 {
                j += 1;
            }

            let mut values = Vec::new();
            for k in i..j {
                values.push(intermediate[k].1);
            }
            let output = reduce(&intermediate[i].0, &values);

            writeln!(&mut outfile, "{} {}", intermediate[i].0, output).unwrap();

            i = j;
        }

        let _ = Service::on_reduce_finished(client, task.task_id).await;
    }
}

async fn process_task(client: &Client, task: Task) {
    if task.is_map() {
        map_task(client, task).await;
    } else {
        reduce_task(client, task).await;
    }
}

pub async fn run() {
    let transporter = Arc::new(Transport::new());
    let client = Client::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        transporter.clone(),
    );

    loop {
        let task_result = match Service::get_task(&client).await {
            Ok(res) => res,
            Err(err) => {
                eprintln!("Unexpected error occured {}", err);
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        match task_result {
            TaskResult::Ready(task) => process_task(&client, task).await,
            TaskResult::Done => std::process::exit(0),
            TaskResult::Pending => std::thread::sleep(Duration::from_secs(1)),
        }
    }
}

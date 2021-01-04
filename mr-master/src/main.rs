mod master;
use master::Master;
use std::collections::HashSet;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: mrsequential inputfiles...\n");
        std::process::exit(1);
    }

    let mut tasks = HashSet::new();

    args.into_iter().for_each(|item| {
        let _ = tasks.insert(item);
    });

    let master = Master::new(tasks);
    master.serve().await;
}

mod master;
use master::Master;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: mr-master inputfiles...\n");
        std::process::exit(1);
    }

    let mut tasks = Vec::new();

    args.into_iter().skip(1).for_each(|item| {
        let _ = tasks.push(item);
    });

    let master = Master::new(tasks, 10);
    master.serve().await;
}

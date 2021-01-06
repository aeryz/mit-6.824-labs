use {
    srpc::{client::Client, transport::Transport},
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    },
};

mod worker;
use worker::*;

#[srpc::client]
trait Service {
    async fn get_task() -> Option<String>;
}

#[tokio::main]
async fn main() {
    run().await;
}

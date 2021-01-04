use {
    srpc::{client::Client, transport::Transport},
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    },
};

#[srpc::client]
trait Service {
    async fn get_task() -> Option<String>;
}

#[tokio::main]
async fn main() {
    let transporter = Arc::new(Transport::new());
    let client = Client::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        transporter.clone(),
    );

    let res = Service::get_task(&client).await.unwrap();
    println!("Got task: {:?}", res);
}

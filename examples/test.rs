use futures::{AsyncBufReadExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use log::{logger, Level, Metadata, Record};
use log::{LevelFilter, SetLoggerError};
const SERF_ADDRESS: &'static str = "0.0.0.0";
const SERF_PORT: &'static str = ":7373";

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger;

pub fn init() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info))
}

#[tokio::main]
async fn main() {
    init();
    let mut serf_address: String = SERF_ADDRESS.into();
    serf_address.push_str(SERF_PORT);
    let socket = serf_address
        .parse()
        .expect("Invalid serf IP address provided");
    let client = serf_rpc::Client::connect(socket, None).await;
    let client = client.unwrap();
    let members = client.members().await.unwrap();

    println!("{:?}", members);

    let current_node_name = client.current_node_name().await.unwrap();

    println!("we are node {}", current_node_name);

    let current_coord = client
        .get_coordinate(&current_node_name)
        .await
        .unwrap()
        .coord
        .unwrap();

    for member in members.members {
        let coord = client
            .get_coordinate(&member.name)
            .await
            .unwrap()
            .coord
            .unwrap();

        println!(
            "estimated rtt to {}: {}s",
            member.name,
            coord.estimate_rtt(&current_coord).as_secs_f32()
        );
    }

    let mut test = serf_rpc::Client::stream(&Arc::new(client), "member-failed");
    let mut result = test.take(1);
    println!(
        "{:?}",
        result.collect::<Vec<_>>().await[0].as_ref().unwrap()
    );
}

use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let socket = "0.0.0.0:7373".parse::<SocketAddr>().unwrap();
    let client = serf_rpc::Client::connect(socket, None).await.unwrap();

    let mut members = client.members().await.unwrap();

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
}

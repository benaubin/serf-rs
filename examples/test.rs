use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let socket = "0.0.0.0:7373".parse::<SocketAddr>().unwrap();
    let client = serf_rpc::RPCClient::connect(socket, None).await.unwrap();

    let mut members = client.members().await.unwrap();

    println!("{:?}", members);


    println!("{:?}", client.members_filtered(None, Some("f33ebcf6e036"), None).await.unwrap());

    let coord = members.members.pop().unwrap().coordinate(&client).await.unwrap().unwrap();

    for member in members.members {
        let coord = client.get_coordinate(&member.name).await.unwrap().coord.unwrap();

        println!("estimated rtt to {}: {}s", member.name, coord.estimate_rtt(&coord).as_secs_f32());
    }
}
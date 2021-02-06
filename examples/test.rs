use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let socket = "0.0.0.0:7373".parse::<SocketAddr>().unwrap();
    let client = serf_rpc::RPCClient::connect(socket, None).await.unwrap();

    let members = client.members().await.unwrap();

    println!("{:?}", members);


    println!("{:?}", client.members_filtered(None, Some("f33ebcf6e036"), None).await.unwrap());

    println!("{:?}", client.get_coordinate("f33ebcf6e036").await.unwrap())
}
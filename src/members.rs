use crate::{RPCClient, RPCResult, protocol::{Coordinate, Member}};


impl Member {
    pub async fn coordinate(&self, client: &RPCClient) -> RPCResult<Option<Coordinate>> {
        let res = client.get_coordinate(&self.name).await?;
        Ok(res.coord)
    }
}
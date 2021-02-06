use std::collections::{HashMap};

use serde::{Deserialize, Serialize};

use crate::{RPCResponse, RPCResult};

#[derive(Serialize)]
pub(crate) struct RequestHeader {
    pub seq: u64,
    pub command: &'static str
}

#[derive(Deserialize)]
pub(crate) struct ResponseHeader {
    pub seq: u64,
    pub error: String
}

macro_rules! count {
    () => { 0 };
    ($item:tt) => {1};
    ($item:tt$(, $rest:tt)+) => { count!( $($rest),+ ) + 1 }
}

macro_rules! cmd_arg {
    (
        $buf:expr,
        $($key:literal: $val:expr),*
    ) => {{
        let len: u32 = count!( $($key),* );

        rmp::encode::write_map_len($buf, len).unwrap();
        $(
            rmp::encode::write_str($buf, $key).unwrap();
            rmp_serde::encode::write($buf, $val).unwrap();
        )*
    }};
}

macro_rules! req {
    (
        $name:literal

        $vis:vis $ident:ident( $($arg:ident: $arg_ty:ty),* ) -> $res:ty $({
            $($key:literal: $val:expr),*
        })?
    ) => {
        impl crate::RPCClient {
            $vis fn $ident<'a>(&'a self$(, $arg: $arg_ty)*) -> crate::RPCRequest<'a, $res> {
                #[allow(unused_mut)]
                let mut buf = Vec::new();

                $(cmd_arg! { &mut buf, $($key: $val),* };)?

                self.request($name, buf)
            }
        }
    };
}

macro_rules! stream {
    (
        $name:literal

        $vis:vis $ident:ident( $($arg:ident: $arg_ty:ty),* ) -> $res:ty $({
            $($key:literal: $val:expr),*
        })?
    ) => {
        impl crate::RPCClient {
            $vis fn $ident(self: &std::sync::Arc<Self>$(, $arg: $arg_ty)*) -> crate::RPCStream<$res> {
                #[allow(unused_mut)]
                let mut buf = Vec::new();

                $(cmd_arg! { &mut buf, $($key: $val),* };)?

                self.start_stream($name, buf)
            }
        }
    };
}

macro_rules! res {
    ($ty:ty) => {
        impl RPCResponse for $ty {
            fn read_from(read: crate::SeqRead<'_>) -> RPCResult<Self> {
                read.read_msg().map_err(|err| err.to_string())
            }
        }
    };
}


req! {
    "handshake"
    pub(crate) handshake(version: u32) -> () {
        "Version": &version
    }
}

req! {
    "auth"
    pub(crate) auth(auth_key: &str) -> () {
        "AuthKey": auth_key
    }
}

req! {
    "event"
    pub fire_event(name: &str, payload: &[u8], coalesce: bool) -> () {
        "Name": name,
        "Payload": payload,
        "Coalesce": &coalesce
    }
}

req! {
    "force-leave"
    pub force_leave(node: &str) -> () {
        "Node": node
    }
}

#[derive(Deserialize)]
pub struct JoinResponse {
    #[serde(rename = "Num")]
    pub nodes_joined: u64
}

res!(JoinResponse);

req! {
    "join"
    pub join(existing: &[&str], replay: bool) -> JoinResponse {
        "Existing": existing,
        "Replay": &replay
    }
}

#[derive(Deserialize)]
pub struct Member {
    #[serde(rename="Name")]
    pub name: String,
    #[serde(rename="Addr")]
    pub addr: [u8; 4],
    #[serde(rename="Port")]
    pub port: u32,
    #[serde(rename="Tags")]
    pub tags: HashMap<String, String>,
    #[serde(rename="Status")]
    pub status: String,
    #[serde(rename="ProtocolMin")]
    pub protocol_min: u32,
    #[serde(rename="ProtocolMax")]
    pub protocol_max: u32,
    #[serde(rename="DelegateCur")]
    pub delegate_cur: u32
}

#[derive(Deserialize)]
pub struct MembersResponse {
    #[serde(rename = "Members")]
    pub members: Vec<Member>
}

res!(MembersResponse);

req! {
    "members"
    pub members() -> MembersResponse
}

req! {
    "members-filtered"
    pub members_filtered(status: Option<&str>, name: Option<&str>, tags: Option<&HashMap<String, String>>) -> MembersResponse {
        "Status": &status,
        "Name": &name,
        "Tags": &tags
    }
}

req! {
    "tags"
    pub tags(add_tags: &[&str], delete_tags: &[&str]) -> MembersResponse {
        "Tags": add_tags,
        "DeleteTags": delete_tags
    }
}

req! {
    "stop"
    pub(crate) stop_stream(seq: u64) -> () {
        "Stop": &seq
    }
}

req! {
    "leave"
    pub leave() -> ()
}

req! {
    "respond"
    pub query_respond(id: u64, payload: &[u8]) -> () {
        "ID": &id,
        "Payload": payload
    }
}

#[derive(Deserialize)]
pub struct Coordinate {
    #[serde(rename="Adjustment")]
    pub adjustment: f32,
    #[serde(rename="Error")]
    pub error: f32,
    #[serde(rename="Height")]
    pub height: f32,
    #[serde(rename="Vec")]
    pub vec: [f32; 8]
}

#[derive(Deserialize)]
pub struct CoordinateResponse {
    #[serde(rename = "Ok")]
    pub ok: bool,

    #[serde(rename = "Coord", default)]
    pub coord: Option<Coordinate>
}

res!(CoordinateResponse);

req! {
    "get-coordinate"
    pub get_coordinate(node: &str) -> CoordinateResponse {
        "Node": node
    }
}


// TODO: STREAM, MONITOR, QUERY

#[derive(Deserialize)]
#[serde(tag = "Event")]
pub enum StreamMessage {
    #[serde(rename="user")]
    User {
        #[serde(rename="LTime")]
        ltime: u64,
        #[serde(rename="Name")]
        name: String,
        #[serde(rename="Payload")]
        payload: Vec<u8>,
        #[serde(rename="Coalesce")]
        coalesce: bool
    },
    #[serde(rename="member-join")]
    MemberJoin {
        #[serde(rename="Members")]
        members: Vec<Member>
    },
    Query {
        #[serde(rename="ID")]
        id: u64,
        #[serde(rename="LTime")]
        ltime: u64,
        #[serde(rename = "Name")]
        name: String,
        #[serde(rename="Payload")]
        payload: Vec<u8>
    }

}
res!(StreamMessage);

stream! {
    "stream"
    pub stream(ty: &str) -> StreamMessage {
        "Type": ty
    }
}

// TODO: query
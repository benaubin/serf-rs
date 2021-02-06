use std::{collections::{HashMap, VecDeque}, fmt::Debug, net::{SocketAddr, TcpStream}, task::{Poll, Waker}};

use std::sync::{Mutex, Arc};
use std::io;

use futures::{Stream};

use std::future::Future;

use io::{BufReader, Write};
use protocol::{RequestHeader};
use serde::{Deserializer, de::DeserializeOwned};

const MAX_IPC_VERSION: u32 = 1;

mod coordinates;
mod members;

pub mod protocol;


pub struct SeqRead<'a>(&'a mut BufReader<TcpStream>);
impl<'a> SeqRead<'a> {
    fn read_msg<T: DeserializeOwned + Debug>(mut self) -> T {
        // annoyingly, we pretty much have to panic, because otherwise the reader is left in an invalid state
        rmp_serde::from_read(&mut self.0).unwrap()
    }
}

trait SeqHandler: 'static + Send + Sync {
    fn handle(&self, res: RPCResult<SeqRead>);
    /// are we expecting more than one response?
    fn streaming(&self) -> bool { false }
}

type RPCResult<T = ()> = Result<T, String>;

#[derive(Clone)]
pub struct RPCClient {
    dispatch: Arc<Mutex<DispatchMap>>,
    tx: std::sync::mpsc::Sender<Vec<u8>>
}

struct DispatchMap {
    map: HashMap<u64, Arc<dyn SeqHandler>>,
    next_seq: u64
}

impl RPCClient {
    /// Connect to hub.
    ///
    /// Waits for handshake, and optionally for authentication if an auth key is provided.
    pub async fn connect(rpc_addr: SocketAddr, auth_key: Option<&str>) -> RPCResult<Self> {
        let (tx, rx) = std::sync::mpsc::channel();

        let dispatch = Arc::new(Mutex::new(DispatchMap {
            map: HashMap::new(),
            next_seq: 0
        }));

        let client = RPCClient {
            dispatch,
            tx
        };

        let dispatch = Arc::downgrade(&client.dispatch);

        std::thread::spawn(move || {
            let mut stream = TcpStream::connect(rpc_addr).unwrap();

            // clone the stream to create a reader
            let mut reader = BufReader::new(stream.try_clone().unwrap());

            // write loop
            std::thread::spawn(move || {
                while let Ok(buf) = rx.recv() {
                    stream.write_all(&buf).unwrap();
                }
            });

            // read loop
            while let Some(dispatch) = dispatch.upgrade() {
                let protocol::ResponseHeader { seq, error } = rmp_serde::from_read(&mut reader).unwrap();
                
                let seq_handler = {
                    let mut dispatch = dispatch.lock().unwrap();
                    match dispatch.map.get(&seq) {
                        Some(v) => {
                            if v.streaming() {
                                v.clone()
                            } else {
                                dispatch.map.remove(&seq).unwrap()
                            }
                        },
                        None => {
                            // response with no handler, ignore
                            continue;
                        }
                    }
                };

                let res = if error.is_empty() {
                    Ok(SeqRead(&mut reader))
                } else {
                    Err(error)
                };

                seq_handler.handle(res);
            }
        });
        
        client.handshake(MAX_IPC_VERSION).await?;

        if let Some(auth_key) = auth_key {
            client.auth(auth_key).await?;
        }

        return Ok(client);
    }

    fn deregister_seq_handler(&self, seq: u64) -> Option<Arc<dyn SeqHandler>> {
        self.dispatch.lock().unwrap().map.remove(&seq)
    }

    /// Asyncrounously sends a request and waits for a response.
    pub(crate) fn request<'a, R: RPCResponse>(&'a self, name: &'static str, body: Vec<u8>) -> RPCRequest<'a, R> {
        RPCRequest {
            client: self,
            state: Arc::new(Mutex::new(RequestState::Unsent(SerializedCommand { name, body })))
        }
    }

    /// Sends a command and registers a streaming sequence handler.
    ///
    /// Note that the request is sent immediately (asyncronously, but not lazily).
    pub(crate) fn start_stream<R: RPCResponse>(&self, name: &'static str, body: Vec<u8>) -> RPCStream<R> {
        let handler = Arc::new(Mutex::new(RPCStreamHandler {
            waker: None,
            queue: VecDeque::new()
        }));
        
        let seq = self.send_command(SerializedCommand { name, body }, Some(handler.clone()));

        RPCStream {
            seq,
            client: self.clone(),
            handler
        }
    }

    /// Send a command, optionally registering a handler for responses.
    ///
    /// Returns the sequence number.
    fn send_command(&self, cmd: SerializedCommand, handler: Option<Arc<dyn SeqHandler>>) -> u64 {
        let seq = {
            let mut dispatch = self.dispatch.lock().unwrap();

            let seq = dispatch.next_seq;
            dispatch.next_seq += 1;

            if let Some(handler) = handler {
                dispatch.map.insert(seq, handler);
            }

            seq
        };

        let mut buf = rmp_serde::encode::to_vec_named(&RequestHeader { command: cmd.name, seq }).unwrap();
        buf.extend_from_slice(&cmd.body);

        self.tx.send(buf).unwrap();

        seq
    }

    pub async fn current_node_name(&self) -> RPCResult<String> {
        Ok(self.stats().await?.agent.name)
    }
}

struct SerializedCommand {
    name: &'static str,
    body: Vec<u8>
}

pub trait RPCResponse: Sized + Send + 'static {
    fn read_from(read: SeqRead<'_>) -> RPCResult<Self>;
}

impl RPCResponse for () {
    fn read_from(_: SeqRead<'_>) -> RPCResult<Self> { Ok(()) }
}

pub struct RPCRequest<'a, R: RPCResponse> {
    client: &'a RPCClient,
    state: Arc<Mutex<RequestState<R>>>
}

enum RequestState<R: RPCResponse> {
    Unsent(SerializedCommand),
    Pending(Waker),
    Ready(RPCResult<R>),
    Invalid
}

impl<'a, T: RPCResponse> RPCRequest<'a, T> {
    fn send_ignored(self) {
        match std::mem::replace(&mut *self.state.lock().unwrap(), RequestState::Invalid) {
            RequestState::Unsent(cmd) => {
                self.client.send_command(cmd, None);
            },
            _ => {
                panic!()
            }
        }
    }
}

impl<'a, T: RPCResponse> Future for RPCRequest<'a, T> {
    type Output = RPCResult<T>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();

        match std::mem::replace(&mut *state, RequestState::Invalid) {
            RequestState::Unsent(cmd) => {
                *state = RequestState::Pending(cx.waker().clone());
                self.client.send_command(cmd, Some(self.state.clone()));
                return Poll::Pending;
            },
            RequestState::Pending(_) => {
                *state = RequestState::Pending(cx.waker().clone());
                return Poll::Pending;
            }
            RequestState::Ready(response) => {
                return Poll::Ready(response);
            },
            RequestState::Invalid => {
                panic!()
            }
        }
    }
}

impl<T> SeqHandler for Mutex<RequestState<T>> where T: RPCResponse {
    fn handle(&self, res: RPCResult<SeqRead>) {
        let res = res.and_then(T::read_from);
        let ready = RequestState::Ready(res);

        match std::mem::replace(&mut *self.lock().unwrap(), ready) {
            RequestState::Pending(waker) => {
                waker.wake()
            },
            _ => panic!()
        }
    }
}

pub struct RPCStream<R: RPCResponse> {
    client: RPCClient,
    seq: u64,
    handler: Arc<Mutex<RPCStreamHandler<R>>>
}

struct RPCStreamHandler<R: RPCResponse> {
    waker: Option<Waker>,
    queue: VecDeque<RPCResult<R>>
}

impl<T: RPCResponse> SeqHandler for Mutex<RPCStreamHandler<T>> {
    fn handle(&self, res: RPCResult<SeqRead>) {
        let RPCStreamHandler { waker, queue } = &mut *self.lock().unwrap();

        let res = res.and_then(T::read_from);
        queue.push_back(res);

        if let Some(waker) = waker.take() { waker.wake() }
    }
    fn streaming(&self) -> bool { true }
}

impl<T: RPCResponse> Drop for RPCStream<T> {
    fn drop(&mut self) {
        self.client.deregister_seq_handler(self.seq);
        self.client.stop_stream(self.seq).send_ignored();
    }
}

impl<C: RPCResponse> Stream for RPCStream<C> {
    type Item = RPCResult<C>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let RPCStreamHandler { waker, queue } = &mut *self.handler.lock().unwrap();

        if let Some(res) = queue.pop_front() { return Poll::Ready(Some(res)) };

        waker.replace(cx.waker().clone());

        Poll::Pending
    }
}



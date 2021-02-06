use std::{sync::{Mutex, Arc}, task::{Poll, Waker}, future::Future};

use crate::{Client, RPCResponse, RPCResult, SeqHandler, SeqRead, SerializedCommand};

impl Client {
    /// Asyncrounously sends a request and waits for a response.
    pub(crate) fn request<'a, R: RPCResponse>(&'a self, name: &'static str, body: Vec<u8>) -> RPCRequest<'a, R> {
        RPCRequest {
            client: self,
            state: Arc::new(Mutex::new(RequestState::Unsent(SerializedCommand { name, body })))
        }
    }
}

pub struct RPCRequest<'a, R: RPCResponse> {
    client: &'a Client,
    state: Arc<Mutex<RequestState<R>>>
}

enum RequestState<R: RPCResponse> {
    Unsent(SerializedCommand),
    Pending(Waker),
    Ready(RPCResult<R>),
    Invalid
}

impl<'a, T: RPCResponse> RPCRequest<'a, T> {
    /// Send this request, but ignore the response
    pub fn send_ignored(self) {
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

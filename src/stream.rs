use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

use futures::Stream;

use crate::{Client, RPCResponse, RPCResult, SeqHandler, SeqRead, SerializedCommand};

impl Client {
    /// Sends a command and registers a streaming sequence handler.
    ///
    /// Note that the request is sent immediately (asyncronously, but not lazily).
    pub(crate) fn start_stream<R: RPCResponse>(
        &self,
        name: &'static str,
        body: Vec<u8>,
    ) -> RPCStream<R> {
        let handler = Arc::new(Mutex::new(RPCStreamHandler {
            waker: None,
            queue: VecDeque::new(),
            acked: false,
        }));

        let seq = self.send_command(SerializedCommand { name, body }, Some(handler.clone()));

        RPCStream {
            seq,
            client: self.clone(),
            handler,
        }
    }
}

pub struct RPCStream<R: RPCResponse> {
    client: Client,
    seq: u64,
    handler: Arc<Mutex<RPCStreamHandler<R>>>,
}

pub(crate) struct RPCStreamHandler<R: RPCResponse> {
    waker: Option<Waker>,
    queue: VecDeque<RPCResult<R>>,
    acked: bool,
}

impl<T: RPCResponse> SeqHandler for Mutex<RPCStreamHandler<T>> {
    fn handle(&self, res: RPCResult<SeqRead>) {
        let RPCStreamHandler { waker, queue,acked } = &mut *self.lock().unwrap();

        let res = res.and_then(T::read_from);
        queue.push_back(res);

        if let Some(waker) = waker.take() {
            waker.wake()
        }
    }
    fn streaming(&self) -> bool {
        true
    }

    fn stream_acked(&self) -> bool {
        let mut handler =&mut self.lock().unwrap();
        let status = handler.acked;
        handler.acked = true;
        status
    }
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
        let RPCStreamHandler { waker, queue,acked } = &mut *self.handler.lock().unwrap();

        if let Some(res) = queue.pop_front() {
            return Poll::Ready(Some(res));
        };

        waker.replace(cx.waker().clone());

        Poll::Pending
    }
}

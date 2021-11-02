use std::{
    task::Poll,
    sync::{Arc, Mutex},
    collections::HashMap,
    pin::Pin,
};
use futures::{Future, task::{Context, Waker}};
use crate::kvraft::server::State;


pub(crate) struct KvOutput<S: State> {
    /// seq -> Output
    pub(crate) output: HashMap<usize, S::Output>,
    /// seq -> Waker
    pub(crate) waker: HashMap<usize, Waker>,
}

impl<S: State> Default for KvOutput<S> {
    fn default() -> Self {
        Self {
            output: HashMap::new(),
            waker: HashMap::new(),
        }
    }
}

pub(crate) struct ServerFuture<S: State> {
    seq: usize,
    pub(crate) res: Arc<Mutex<KvOutput<S>>>,
}

impl<S: State> ServerFuture<S> {
    pub(crate) fn new(
        seq: usize,
        res: Arc<Mutex<KvOutput<S>>>,
    ) -> Self {
        Self { seq, res }
    }
}

impl<S: State> Future for ServerFuture<S> {
    type Output = S::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut res = self.res.lock().unwrap();
        if let Some(output) = res.output.get(&self.seq) {
            Poll::Ready(output.clone())
        } else {
            res.waker.insert(self.seq, cx.waker().clone());
            Poll::Pending
        }
    }
}
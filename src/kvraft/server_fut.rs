use crate::kvraft::state::State;
use futures::{
    task::{Context, Waker},
    Future,
};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

pub(crate) struct Output<S: State> {
    /// seq -> Output
    pub(crate) output: HashMap<usize, S::Output>,
    /// seq -> Waker
    pub(crate) waker: HashMap<usize, Waker>,
}

impl<S: State> Default for Output<S> {
    fn default() -> Self {
        Self {
            output: HashMap::new(),
            waker: HashMap::new(),
        }
    }
}

pub(crate) struct ServerFuture<S: State> {
    seq: usize,
    pub(crate) res: Arc<Mutex<Output<S>>>,
}

impl<S: State> ServerFuture<S> {
    pub(crate) fn new(seq: usize, res: Arc<Mutex<Output<S>>>) -> Self {
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

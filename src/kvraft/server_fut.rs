use std::{task::Poll, sync::{Arc, Mutex}, collections::HashMap, pin::Pin};
use futures::{Future, task::{Context, Waker}};
use crate::kvraft::server::State;


pub(crate) struct KvOutput<S: State> {
    /// seq -> Output
    pub(crate) output: HashMap<usize, S::Output>,
    /// (term, index) -> Waker
    pub(crate) waker: HashMap<(u64, usize), Option<Waker>>,
    /// (term, index) -> seq
    pub(crate) log_2_seq: HashMap<(u64, usize), usize>,
}

impl<S: State> Default for KvOutput<S> {
    fn default() -> Self {
        Self {
            output: HashMap::new(),
            waker: HashMap::new(),
            log_2_seq: HashMap::new(),
        }
    }
}

pub(crate) struct ServerFuture<S: State> {
    term: u64,
    index: usize,
    pub(crate) res: Arc<Mutex<KvOutput<S>>>,
}

impl<S: State> ServerFuture<S> {
    pub(crate) fn new(
        seq: usize,
        term: u64,
        index: usize,
        res: Arc<Mutex<KvOutput<S>>>
    ) -> Self {
        {
            let mut kv = res.lock().unwrap();
            kv.log_2_seq.insert((term, index), seq);
            kv.waker.insert((term, index), None);
        }
        Self { term, index, res }
    }
}

impl<S: State> Future for ServerFuture<S> {
    type Output = S::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut res = self.res.lock().unwrap();
        let &seq = res.log_2_seq.get(&(self.term, self.index)).unwrap();
        if let Some(output) = res.output.get(&seq) {
            Poll::Ready(output.clone())
        } else {
            res.waker.insert((self.term, self.index), Some(cx.waker().clone()));
            Poll::Pending
        }
    }
}
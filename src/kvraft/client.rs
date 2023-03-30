use crate::kvraft::msg::*;
use madsim::{
    net,
    rand::{self, Rng},
    time::*,
};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

const CLIENT_TIMEOUT: Duration = Duration::from_millis(500);

pub struct Clerk {
    core: ClerkCore<Op, String>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            core: ClerkCore::new(servers),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    pub async fn get(&self, key: String) -> String {
        self.core.call(Op::Get { key }).await
    }

    pub async fn put(&self, key: String, value: String) {
        self.core.call(Op::Put { key, value }).await;
    }

    pub async fn append(&self, key: String, value: String) {
        self.core.call(Op::Append { key, value }).await;
    }
}

pub struct ClerkCore<Req, Rsp> {
    me: usize,
    leader: AtomicUsize,
    servers: Vec<SocketAddr>,
    _mark: std::marker::PhantomData<(Req, Rsp)>,
}

impl<Req, Rsp> ClerkCore<Req, Rsp>
where
    Req: net::Message + Clone,
    Rsp: net::Message,
{
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        ClerkCore {
            me: rand::rng().gen::<usize>(),
            leader: AtomicUsize::new(0),
            servers,
            _mark: PhantomData,
        }
    }

    pub async fn call(&self, args: Req) -> Rsp {
        let net = net::NetLocalHandle::current();
        let seq = rand::rng().gen::<usize>();
        let mut cur = self.leader.load(Ordering::Relaxed);
        let args = Msg {
            client: self.me,
            seq,
            data: args,
        };
        loop {
            let ret = net
                .call_timeout::<Msg<Req>, Result<Rsp, Error>>(
                    self.servers[cur],
                    args.clone(),
                    CLIENT_TIMEOUT,
                )
                .await;
            match ret {
                // Success
                Ok(Ok(res)) => {
                    self.leader.store(cur, Ordering::Relaxed);
                    return res;
                }
                Ok(Err(e)) => {
                    cur = match e {
                        // The server is not Leader.
                        Error::NotLeader { hint } => hint,
                        // Failed to reach consensus,
                        // i.e. the log entry has been over-written.
                        Error::Failed => (cur + 1) % self.servers.len(),
                        // Server timeout, added to log but not committed yet.
                        // CAUTION: Leader of a minority partition.
                        Error::Timeout => (cur + 1) % self.servers.len(),
                    }
                }
                // Client timeout, due to server crash or packet loss.
                Err(_) => cur = (cur + 1) % self.servers.len(),
            }
        }
    }
}

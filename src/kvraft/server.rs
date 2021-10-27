use crate::{kvraft::{msg::*, server_fut::*}, raft};
use madsim::{net, task, time};
use serde::{Deserialize, Serialize};
use futures::{StreamExt, channel::mpsc::UnboundedReceiver};
use std::{collections::HashMap, fmt::{self, Debug}, net::SocketAddr, sync::{Arc, Mutex}, time::Duration};


const SERVER_TIMEOUT: Duration = Duration::from_millis(400);


pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message + Clone;
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerCommand<S: State> {
    seq: usize,
    command: S::Command,
}

pub struct Server<S: State> {
    raft: raft::RaftHandle,
    me: usize,
    state: Arc<Mutex<S>>,
    res: Arc<Mutex<KvOutput<S>>>,
}

impl<S: State> fmt::Debug for Server<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Server({})", self.me)
    }
}

impl<S: State> Server<S> {
    pub async fn new(
        servers: Vec<SocketAddr>,
        me: usize,
        _max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        let (raft, apply_ch) = raft::RaftHandle::new(
            servers, me
        ).await;

        let this = Arc::new(Server { 
            raft, 
            me, 
            state: Arc::new(Mutex::new(S::default())), 
            res: Arc::new(Mutex::new(KvOutput::default())),
        });

        this.start_rpc_server();
        this.start_listen_channel(apply_ch);
        this
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |msg: Msg<S::Command>| {
            let this = this.clone();
            async move { this.apply(msg.seq, msg.data).await }
        });
    }

    fn start_listen_channel(
        self: &Arc<Self>, 
        mut apply_ch: UnboundedReceiver<raft::ApplyMsg>,
    ) {
        let this = self.clone();
        task::spawn(async move {
            while let Some(cmd) = apply_ch.next().await {
                match cmd {
                    raft::ApplyMsg::Command { data, term, index } => {
                        let cmd: ServerCommand<S> = bincode::deserialize(&data).unwrap();
                        let ServerCommand { seq, command } = cmd;
                        // info!("[{:?}] commit index {}: {:?}", this, index, command);
                        let mut kv_output = this.res.lock().unwrap();

                        let output = kv_output
                            .output
                            .get(&seq)
                            .map(|v| v.clone())
                            .unwrap_or_else(|| {
                                this.state.lock().unwrap().apply(command)
                            });

                        kv_output.output.insert(seq, output);
                        if let Some(waker) = kv_output.waker.get_mut(&(term, index)) {
                            let waker = waker.take().unwrap();
                            waker.wake();
                        }
                    },
                    raft::ApplyMsg::Snapshot { data, index, .. } => {
                        let state: S = bincode::deserialize(&data).unwrap();
                        // info!("[{:?}] snapshot index {}", this, index);
                        *this.state.lock().unwrap() = state;
                    },
                }
            }
        }).detach();
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    async fn apply(&self, seq: usize, cmd: S::Command) -> Result<S::Output, Error> {
        // Repeat request
        if let Some(res) = self.res.lock().unwrap().output.get(&seq) {
            return Ok(res.clone());
        }
        if self.raft.is_leader() {
            let cmd: ServerCommand<S> = ServerCommand { seq, command: cmd };
            match self.raft.start(&bincode::serialize(&cmd).unwrap()).await {
                Ok(raft::Start { term, index }) => {
                    // info!("[{:?}] start (seq {}) index {}, term {}, cmd {:?}",
                    //     self, seq, index, term, cmd);
                    let f = ServerFuture::new(
                        seq, term, index, self.res.clone(),
                    );
                    time::timeout(SERVER_TIMEOUT, f)
                        .await
                        .map_err(|e| Error::Timeout)
                },
                Err(e) => match e {
                    raft::Error::NotLeader(hint) => Err(Error::NotLeader { hint }),
                    raft::Error::IO(_) => unreachable!(),
                },
            }
        } else {
            Err(Error::NotLeader { hint: self.raft.leader() })
        }
    }
}

pub type KvServer = Server<Kv>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Kv {
    data: HashMap<String, String>,
}

impl State for Kv {
    type Command = Op;
    type Output = String;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Get { key } => self.get(key),
            Op::Put { key, value } => self.put(key, value),
            Op::Append { key, value } => self.append(key, value),
        }
    }
}

impl Kv {
    fn get(&self, key: String) -> String {
        if let Some(v) = self.data.get(&key) {
            v.into()
        } else {
            "".to_string()
        }
    }

    fn put(&mut self, key: String, value: String) -> String {
        self.data.insert(key, value).unwrap_or("".to_string())
    }

    fn append(&mut self, key: String, value: String) -> String {
        if let Some(v) = self.data.get_mut(&key) {
            v.push_str(&value);
            v.as_str().into()
        } else {
            "".to_string()
        }
    }
}

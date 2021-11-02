use crate::{kvraft::{msg::*, server_fut::*}, raft};
use madsim::{fs, net, task, time};
use serde::{Deserialize, Serialize};
use futures::{StreamExt, channel::mpsc::UnboundedReceiver};
use std::{
    io,
    fmt::{self, Debug},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
    collections::HashMap,
};


const SERVER_TIMEOUT: Duration = Duration::from_millis(400);

pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message + Clone;
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerCommand<S: State> {
    client: usize,
    seq: usize,
    command: S::Command,
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
// struct Persist {
//     applied: Vec<Option<usize>>,
// }


pub struct Server<S: State> {
    raft: raft::RaftHandle,
    me: usize,
    /// Shared via snapshot
    state: Arc<Mutex<S>>,
    res: Arc<Mutex<KvOutput<S>>>,
    /// Last applied seq number for each client.
    /// Shared via snapshot
    last_applied: Arc<Mutex<HashMap<usize, usize>>>,
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
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        let (raft, apply_ch) = raft::RaftHandle::new(
            servers, me,
        ).await;

        let this = Arc::new(Server {
            raft,
            me,
            state: Arc::new(Mutex::new(S::default())),
            res: Arc::new(Mutex::new(KvOutput::default())),
            last_applied: Arc::new(Mutex::new(HashMap::new())),
        });
        let max_log_size = max_raft_state
            .unwrap_or(usize::MAX);
        this.start_listen_channel(apply_ch, max_log_size);
        this.start_rpc_server();

        this
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();
        let this = self.clone();
        net.add_rpc_handler(move |msg: Msg<S::Command>| {
            let this = this.clone();
            async move { this.apply(msg.client, msg.seq, msg.data).await }
        });
    }

    fn start_listen_channel(
        self: &Arc<Self>,
        mut apply_ch: UnboundedReceiver<raft::ApplyMsg>,
        max_log_size: usize,
    ) {
        let this = self.clone();
        task::spawn(async move {
            while let Some(cmd) = apply_ch.next().await {
                match cmd {
                    raft::ApplyMsg::Command { data, index, .. } => {
                        let cmd: ServerCommand<S> = bincode::deserialize(&data).unwrap();
                        let ServerCommand { client, seq, command } = cmd;
                        let mut snapshot = None;
                        {
                            let mut kv_output = this.res.lock().unwrap();
                            let mut last_applied = this.last_applied.lock().unwrap();

                            // not applied in state machine
                            if Some(&seq) != last_applied.get(&client) {
                                let mut state = this.state.lock().unwrap();
                                debug!("[{:?}] commit index {}, seq {}: {:?}", this, index, seq, command);
                                let output = state.apply(command);
                                last_applied.insert(client, seq);
                                kv_output.output.insert(seq, output);
                                snapshot = if this.raft.log_size() > max_log_size {
                                    Some(bincode::serialize(&(&*state, &*last_applied)).unwrap())
                                } else {
                                    None
                                };
                            }

                            if let Some(waker) = kv_output.waker.remove(&seq) {
                                waker.wake();
                            }
                        }

                        // Snapshotting
                        if let Some(snapshot) = snapshot {
                            this.raft.snapshot(index, &snapshot).await.unwrap();
                        }
                    },
                    raft::ApplyMsg::Snapshot { data, index, term } => {
                        if this.raft.cond_install_snapshot(term, index as u64, &data).await {
                            debug!("[{:?}] snapshot index {}", this, index);
                            let snapshot: (S, HashMap<usize, usize>) = bincode::deserialize(&data).unwrap();
                            *this.state.lock().unwrap() = snapshot.0;
                            *this.last_applied.lock().unwrap() = snapshot.1;
                        }
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

    async fn apply(&self, client: usize, seq: usize, cmd: S::Command) -> Result<S::Output, Error> {
        // Repeat request
        if let Some(res) = self.res.lock().unwrap().output.get(&seq) {
            return Ok(res.clone());
        }
        if self.raft.is_leader() {
            let cmd: ServerCommand<S> = ServerCommand { client, seq, command: cmd };
            match self.raft.start(&bincode::serialize(&cmd).unwrap()).await {
                Ok(raft::Start { term, index }) => {
                    // info!("[{:?}] start (seq {}) index {}, term {}, cmd {:?}",
                    //     self, seq, index, term, cmd);
                    let f = ServerFuture::new(seq,  self.res.clone());
                    time::timeout(SERVER_TIMEOUT, f)
                        .await
                        .map_err(|_| Error::Timeout)
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

    // async fn persist(&self) -> io::Result<()> {
    //     let persist = Persist {
    //         applied: self.applied.clone(),
    //     };
    //     let persist = bincode::serialize(&persist).unwrap();
    //     let paths = ["server1", "server"];
    //     for path in paths {
    //         let file = fs::File::create(path).await?;
    //         file.write_all_at(&persist, 0).await?;
    //         file.sync_all().await?;
    //     }
    //     Ok(())
    // }

    // async fn restore(&mut self) -> io::Result<()> {
    //     let paths = ["server", "server1"];
    //     for path in paths {
    //         match fs::read(path).await {
    //             Ok(persist) => {
    //                 let persist = bincode::deserialize(&persist);
    //                 if persist.is_err() { continue; }
    //                 let persist: Persist = persist.unwrap();
    //                 self.applied = persist.applied;
    //                 return Ok(());
    //             },
    //             Err(e) if e.kind() == io::ErrorKind::NotFound => {},
    //             Err(e) => return Err(e),
    //         }
    //     }
    //     Ok(())
    // }
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
        self.data
            .get(&key)
            .map_or("", |v| v.as_str())
            .to_string()
    }

    fn put(&mut self, key: String, value: String) -> String {
        self.data
            .insert(key, value)
            .unwrap_or("".to_string())
    }

    fn append(&mut self, key: String, value: String) -> String {
        self.data
            .get_mut(&key)
            .map_or("", |v| {
                v.push_str(&value);
                v.as_str()
            })
            .to_string()
    }
}

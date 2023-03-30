use crate::{
    kvraft::{client::ClerkCore, server::Server, state::State},
    shard_ctrler::{
        client::Clerk as CtrlerClerk,
        msg::{Config, ConfigId, Gid},
        N_SHARDS,
    },
    shardkv::{key2shard, msg::*},
};
use futures::{select_biased, stream::FuturesUnordered, StreamExt};
use madsim::{
    task,
    time::{self, Duration},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Formatter},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

pub struct ShardKvServer {
    gid: u64,
    ctrl_ck: CtrlerClerk,
    sevr_ck: Mutex<HashMap<Gid, Arc<ClerkCore<Op, Reply>>>>,
    _inner: Arc<Server<ShardKv>>,
    config: Mutex<Config>,
}

impl ShardKvServer {
    pub async fn new(
        ctrl_ck: CtrlerClerk,
        servers: Vec<SocketAddr>,
        gid: u64,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        let config = ctrl_ck.query().await;
        let _inner = Server::new(servers, me, max_raft_state).await;
        let this = Arc::new(ShardKvServer {
            gid,
            ctrl_ck,
            sevr_ck: Mutex::new(HashMap::new()),
            _inner,
            config: Mutex::new(config),
        });

        this.start_check_config();
        this
    }

    fn start_check_config(self: &Arc<Self>) {
        let this = self.clone();
        task::spawn(async move {
            let mut new_cfg: ConfigId = 0;
            loop {
                let config = this.ctrl_ck.query_at(new_cfg).await;
                let cfg = config.num;
                config.groups.iter().for_each(|(gid, servers)| {
                    if !this.sevr_ck.lock().unwrap().contains_key(gid) {
                        let servers = servers.clone();
                        this.sevr_ck
                            .lock()
                            .unwrap()
                            .insert(*gid, Arc::new(ClerkCore::new(servers)));
                    }
                });
                let prev_shards = this.config.lock().unwrap().shards;
                *this.config.lock().unwrap() = config.clone();

                let mut ask_for_shards = FuturesUnordered::new();
                for (shard, gid) in config.shards.iter().enumerate() {
                    let prev_gid = prev_shards[shard];

                    // a new shard which didn't belong to me
                    if *gid == this.gid && prev_gid != this.gid {
                        let this = this.clone();
                        ask_for_shards.push(async move {
                            let prev_core = this.sevr_ck.lock().unwrap().get(&prev_gid).cloned();

                            let mut skip = false;
                            let data = if let Some(core) = prev_core.clone() {
                                let d: Option<Vec<u8>>;
                                loop {
                                    match core.call(Op::RemoveShard { cfg, shard }).await {
                                        Reply::Shard { data, .. } => {
                                            d = Some(data);
                                            break;
                                        }
                                        Reply::Retry => continue,
                                        Reply::Ok => {
                                            d = None;
                                            skip = true;
                                            break;
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                                d
                            } else {
                                None
                            };

                            if !skip {
                                let my_core =
                                    this.sevr_ck.lock().unwrap().get(&this.gid).unwrap().clone();
                                my_core.call(Op::InstallShard { cfg, shard, data }).await;
                                if let Some(core) = prev_core {
                                    match core.call(Op::ShardInstalled { cfg, shard }).await {
                                        Reply::Ok => {}
                                        _ => unreachable!(),
                                    }
                                }
                            }
                        });
                    }
                }
                loop {
                    select_biased! {
                        _ = ask_for_shards.select_next_some() => continue,
                        complete => break,
                    }
                }
                time::sleep(Duration::from_millis(80)).await;
                new_cfg = cfg + 1;
            }
        })
        .detach();
    }
}

impl fmt::Debug for ShardKvServer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ShardServer({})", self.gid)
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct ShardKv {
    /// shard -> kv
    shard2kv: HashMap<usize, HashMap<String, String>>,
    /// shard -> last ConfigId applied
    shard2cfg: [ConfigId; N_SHARDS],
    /// shard -> contained
    contains: [bool; N_SHARDS],
}

impl fmt::Debug for ShardKv {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.shard2kv.keys())
    }
}

impl State for ShardKv {
    type Command = Op;
    type Output = Reply;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Get { key } => {
                let shard = key2shard(&key);
                if !self.contains[shard] {
                    return Reply::WrongGroup;
                }
                self.shard2kv
                    .get(&shard)
                    .map(|kv| {
                        let value = kv.get(&key).cloned();
                        Reply::Get { value }
                    })
                    .unwrap()
            }
            Op::Put { key, value } => {
                let shard = key2shard(&key);
                if !self.contains[shard] {
                    return Reply::WrongGroup;
                }
                self.shard2kv
                    .get_mut(&shard)
                    .map(|kv| {
                        kv.insert(key, value);
                        Reply::Ok
                    })
                    .unwrap()
            }
            Op::Append { key, value } => {
                let shard = key2shard(&key);
                if !self.contains[shard] {
                    return Reply::WrongGroup;
                }
                self.shard2kv
                    .get_mut(&shard)
                    .map(|kv| {
                        if let Some(v) = kv.get_mut(&key) {
                            v.push_str(&value)
                        }
                        Reply::Ok
                    })
                    .unwrap()
            }
            Op::InstallShard { cfg, shard, data } => {
                if cfg > self.shard2cfg[shard] {
                    let kv: HashMap<String, String> =
                        data.map_or(HashMap::new(), |d| bincode::deserialize(&d).unwrap());
                    self.shard2kv.insert(shard, kv);
                    self.shard2cfg[shard] = cfg;
                    self.contains[shard] = true;
                }
                Reply::Ok
            }
            Op::RemoveShard { cfg, shard } => {
                if cfg > self.shard2cfg[shard] {
                    if let Some(data) = self.shard2kv.get(&shard).cloned() {
                        let data = bincode::serialize(&data).unwrap();
                        self.contains[shard] = false;
                        Reply::Shard { shard, data }
                    } else {
                        Reply::Retry
                    }
                } else {
                    Reply::Ok
                }
            }
            Op::ShardInstalled { cfg, shard } => {
                if cfg > self.shard2cfg[shard] {
                    self.shard2kv.remove(&shard);
                    self.shard2cfg[shard] = cfg;
                }
                Reply::Ok
            }
        }
    }
}

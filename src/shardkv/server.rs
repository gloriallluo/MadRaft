use crate::{
    shardkv::{msg::*, key2shard},
    kvraft::{server::Server, state::State},
    shard_ctrler::client::Clerk as CtrlerClerk,
};
use serde::{Deserialize, Serialize};
use madsim::{task, net, time::{self, Duration}};
use std::{
    net::SocketAddr, 
    sync::Arc,
    collections::HashMap,
};

pub struct ShardKvServer {
    gid: u64,
    ctrl_ck: CtrlerClerk,
    inner: Arc<Server<ShardKv>>,
}

impl ShardKvServer {
    pub async fn new(
        ctrl_ck: CtrlerClerk,
        servers: Vec<SocketAddr>,
        gid: u64,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        let inner = Server::new(servers, me, max_raft_state).await;
        let this = Arc::new(ShardKvServer { 
            gid, 
            ctrl_ck,
            inner, 
        });
        let that = this.clone();
        that.start_check_config();
        let that = this.clone();
        that.start_listen_config();
        this
    }

    fn start_check_config(self: Arc<Self>) {
        task::spawn(async move {
            loop {
                {
                    let config = self.ctrl_ck.query().await;
                    config.shards
                        .iter()
                        .enumerate()
                        .for_each(|(shard, gid)| {
                            if *gid == self.gid {
                                if !self.inner.contains_shard(shard) {
                                    self.inner.allocate_shard(shard);
                                }
                            } else if self.inner.contains_shard(shard) {
                                // TODO: send the shard to the real server group
                            }
                        });
                }
                time::sleep(Duration::from_millis(80)).await;
            }
        }).detach();
    }

    fn start_listen_config(self: Arc<Self>) {
        // let net = net::NetLocalHandle::current();
        // net.add_rpc_handler(move |msg: String| {
        //     let this = self.clone();
        //     async move { }
        // });
    }
}

impl Server<ShardKv> {
    fn contains_shard(&self, shard: usize) -> bool {
        self.state.lock().unwrap().shard2kv.contains_key(&shard)
    }

    fn allocate_shard(&self, shard: usize) {
        self.state.lock().unwrap().shard2kv.insert(shard, HashMap::new());
    }

    fn install_shard(&self, shard: usize, kv: HashMap<String, String>) {
        self.state.lock().unwrap().shard2kv.insert(shard, kv);
    }

    fn remove_shard(&self, shard: usize) -> HashMap<String, String> {
        self.state.lock().unwrap().shard2kv.remove(&shard).unwrap()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    /// shard -> kv
    shard2kv: HashMap<usize, HashMap<String, String>>,
}

impl State for ShardKv {
    type Command = Op;
    type Output = Reply;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Get { key } => {
                let shard = key2shard(&key);
                self.shard2kv
                    .get(&shard)
                    .map_or(
                        Reply::WrongGroup,
                        |kv| {
                            let value = kv
                                .get(&key)
                                .map_or(None, |v| Some(v.clone()));
                            Reply::Get { value }
                        }
                    )
            },
            Op::Put { key, value } => {
                let shard = key2shard(&key);
                self.shard2kv
                    .get_mut(&shard)
                    .map_or(
                        Reply::WrongGroup,
                        |kv| {
                            kv.insert(key, value);
                            Reply::Ok
                        }
                    )
            },
            Op::Append { key, value } => {
                let shard = key2shard(&key);
                self.shard2kv
                    .get_mut(&shard)
                    .map_or(
                        Reply::WrongGroup,
                        |kv| {
                            kv.get_mut(&key).map(|v| v.push_str(&value));
                            Reply::Ok
                        }
                    )
            },
            Op::InstallShard { shard, data } => {
                let kv: HashMap<String, String> = bincode::deserialize(&data).unwrap();
                self.shard2kv.insert(shard, kv);
                Reply::Ok
            },
            Op::RemoveShard { shard } => {
                self.shard2kv.remove(&shard);
                Reply::Ok
            }
        }
    }
}

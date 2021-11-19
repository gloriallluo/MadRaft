use crate::{
    shardkv::msg::*,
    kvraft::client::ClerkCore,
    shard_ctrler::client::Clerk as CtrlerClerk,
};
use std::{
    net::SocketAddr,
    sync::Arc,
    cell::RefCell,
    collections::HashMap,
};

use super::key2shard;


pub struct Clerk {
    /// Communicate with ShardCtrler
    ctrl_ck: CtrlerClerk,
    /// shard -> ClerkCore
    cores: RefCell<HashMap<usize, Arc<ClerkCore<Op, Reply>>>>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            ctrl_ck: CtrlerClerk::new(servers),
            cores: RefCell::new(HashMap::new()),
        }
    }

    pub async fn get(&self, key: String) -> String {
        loop {
            let key = key.clone();
            match self.call(Op::Get { key }).await {
                Reply::Get { value } => return value.unwrap_or("".to_string()),
                Reply::WrongGroup => self.renew_cores().await,
                _ => unreachable!(),
            };
        }
    }

    pub async fn put(&self, key: String, value: String) {
        loop {
            let key = key.clone();
            let value = value.clone();
            match self.call(Op::Put { key, value }).await {
                Reply::Ok => return,
                Reply::WrongGroup => self.renew_cores().await,
                _ => unreachable!(),
            };
        }
    }

    pub async fn append(&self, key: String, value: String) {
        loop {
            let key = key.clone();
            let value = value.clone();
            match self.call(Op::Append { key, value }).await {
                Reply::Ok => return,
                Reply::WrongGroup => self.renew_cores().await,
                _ => unreachable!(),
            };
        }
    }

    async fn call(&self, args: Op) -> Reply {
        let key = match &args {
            Op::Get { key } => key,
            Op::Put { key, .. } => key,
            Op::Append { key, .. } => key,
            _ => unreachable!(),
        };
        let shard = key2shard(key);
        if !self.cores.borrow().contains_key(&shard) {
            self.renew_cores().await;
        }
        let reply = self.cores.borrow().get(&shard).unwrap().call(args).await;
        // debug!("get reply: {:?}", reply);
        reply
    }

    async fn renew_cores(&self) {
        self.cores.borrow_mut().clear();
        let config = self.ctrl_ck.query().await;
        let gid2core = config.groups
            .into_iter()
            .fold(HashMap::new(), |mut map, (gid, servers)| {
                map.insert(
                    gid,
                    Arc::new(ClerkCore::<Op, Reply>::new(servers)),
                );
                map
            });
        config.shards
            .iter()
            .enumerate()
            .for_each(|(shard, gid)| {
                let core = gid2core.get(gid).unwrap().clone();
                self.cores.borrow_mut().insert(shard, core);
            })
    }
}

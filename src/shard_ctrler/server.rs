use std::collections::HashMap;
use crate::{
    shard_ctrler::{msg::*, N_SHARDS},
    kvraft::{server::Server, state::State},
};
use serde::{Deserialize, Serialize};


pub type ShardCtrler = Server<ShardInfo>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardInfo {
    configs: Vec<Config>,
}

impl Default for ShardInfo {
    fn default() -> Self {
        Self {
            configs: vec![Config::default()],
        }
    }
}

impl ShardInfo {
    fn new_config(&self) -> Config {
        self.configs
            .last()
            .map(|config| {
                let mut config = config.clone();
                config.num += 1;
                config
            })
            .unwrap()
    }
}

impl State for ShardInfo {
    type Command = Op;
    type Output = Option<Config>;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        // debug!("apply: {:?}", cmd);
        match cmd {
            Op::Query { num } => {
                if num < self.configs.len() as ConfigId {
                    Some(self.configs[num as usize].clone())
                } else {
                    self.configs.last().map(|v| v.clone())
                }
            },
            Op::Move { shard, gid } => {
                let mut new_config = self.new_config();
                new_config.shards[shard] = gid;
                self.configs.push(new_config.clone());
                Some(new_config)
            },
            Op::Join { groups } => {
                let mut new_config = self.new_config();
                let mut ng = Vec::new();
                groups
                    .into_iter()
                    .for_each(|g| {
                        ng.push(g.0);
                        new_config.groups.insert(g.0, g.1);
                    });
                new_config.balance_join(ng);
                self.configs.push(new_config.clone());
                Some(new_config)
            },
            Op::Leave { gids } => {
                let mut new_config = self.new_config();
                gids
                    .iter()
                    .for_each(|g| {
                        new_config.groups.remove(g);
                    });
                new_config.balance_leave(gids);
                self.configs.push(new_config.clone());
                Some(new_config)
            },
        }
    }
}

impl Config {
    /// Re-balance after join.
    fn balance_join(&mut self, mut new_groups: Vec<Gid>) {
        let n_groups = self.groups.len();
        let opt = N_SHARDS / n_groups;
        // num of groups that hold opt + 1 shards
        let r = N_SHARDS - n_groups * opt;

        // shards which are in full-loaded groups
        let mut re_alloc_shards: Vec<usize> = Vec::new();
        // Gid -> #shards
        let mut count = HashMap::new();

        for (shard, gid) in self.shards.iter().enumerate() {
            if *gid == 0 {
                re_alloc_shards.push(shard);
                continue;
            }
            let &cnt = count.get(gid).unwrap_or(&0usize);
            if cnt + 1 > opt + 1 {
                re_alloc_shards.push(shard);
            } else if cnt + 1 == opt + 1 {
                re_alloc_shards.insert(0, shard);
            }
            count.insert(gid, cnt + 1);
        }

        new_groups.sort();

        new_groups
            .iter()
            .enumerate()
            .for_each(|(i, gid)| {
                let c = if i < r { opt + 1 } else { opt };
                for _ in 0..c {
                    self.shards[re_alloc_shards.pop().unwrap()] = *gid;
                }
            });
    }

    /// Re-balance after leave.
    fn balance_leave(&mut self, old_groups: Vec<Gid>) {
        let n_groups = self.groups.len();
        if n_groups == 0 {
            self.shards.iter_mut().for_each(|g| *g = 0);
            return;
        }
        let opt = N_SHARDS / n_groups;

        // shards assigned to groups to leave
        let mut re_alloc_shards: Vec<usize> = Vec::new();
        // Gid -> #shards
        let mut count = HashMap::new();

        for (shard, gid) in self.shards.iter().enumerate() {
            if old_groups.contains(gid) {
                re_alloc_shards.push(shard);
                continue;
            }
            let &cnt = count.get(gid).unwrap_or(&0usize);
            count.insert(gid, cnt + 1);
        }

        let mut re_alloc_groups: Vec<Gid> = Vec::new();
        let mut all_groups: Vec<Gid> = self.groups
            .iter()
            .map(|v| *v.0)
            .collect();

        all_groups.sort();

        for gid in all_groups {
            let cnt = count.get(&gid).map_or(0, |v| *v);
            for _ in cnt..opt {
                re_alloc_groups.push(gid);
            }
            if cnt < opt + 1 {
                re_alloc_groups.insert(0, gid);
            }
        }

        re_alloc_shards
            .iter()
            .for_each(|&shard| {
                self.shards[shard] = re_alloc_groups.pop().unwrap();
            });
    }
}

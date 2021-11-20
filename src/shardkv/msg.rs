use serde::{Deserialize, Serialize};
use crate::shard_ctrler::msg::ConfigId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Append {
        key: String,
        value: String,
    },
    InstallShard {
        cfg: ConfigId,
        shard: usize,
        data: Option<Vec<u8>>,
    },
    RemoveShard {
        cfg: ConfigId,
        shard: usize,
    },
    ShardInstalled {
        cfg: ConfigId,
        shard: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Get {
        value: Option<String>,
    },
    Ok,
    Retry,
    WrongGroup,
    Shard {
        shard: usize,
        data: Vec<u8>,
    },
}

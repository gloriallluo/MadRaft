use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Get { key: String },
    Put { key: String, value: String },
    Append { key: String, value: String },
    InstallShard { shard: usize, data: Vec<u8> },
    RemoveShard { shard: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Get { value: Option<String> },
    Ok,
    WrongGroup,
}

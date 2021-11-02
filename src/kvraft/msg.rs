use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Msg<T> {
    pub(crate) client: usize,
    pub(crate) seq: usize,
    pub(crate) data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Get { key: String },
    Put { key: String, value: String },
    Append { key: String, value: String },
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    #[error("not leader, hint: {hint}")]
    NotLeader { hint: usize },
    #[error("server timeout")]
    Timeout,
    #[error("failed to reach consensus")]
    Failed,
}


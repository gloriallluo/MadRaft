use crate::raft::log::LogEntry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub(crate) term: u64,
    pub(crate) candidate: usize,
    pub(crate) last_log_term: u64,
    pub(crate) last_log_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub(crate) term: u64,
    pub(crate) vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntryArgs {
    pub(crate) term: u64,
    pub(crate) leader: usize,
    pub(crate) prev_log_index: usize,
    pub(crate) prev_log_term: u64,
    pub(crate) log_entries: Vec<LogEntry>,
    pub(crate) leader_commit_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntryReply {
    pub(crate) term: u64,
    pub(crate) success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    pub(crate) term: u64,
    pub(crate) leader: usize,
    pub(crate) last_included_term: u64,
    pub(crate) last_included_index: usize,
    pub(crate) offset: usize,
    pub(crate) done: bool,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub(crate) term: u64,
}

use futures::{channel::mpsc, stream::FuturesUnordered, Future};
use madsim::{net, time::*};
use std::{
    fmt, io,
    net::SocketAddr,
    cmp::min,
};
use crate::raft::{
    args::*, log::*,
    raft_handle::{Result, RPCResult},
};
use serde::{Deserialize, Serialize};
use std::cmp::max;


type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: usize,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: usize,
    },
}


pub const SNAPSHOT_SIZE: usize = 256; // 256 bytes
const RPC_TIMEOUT: Duration = Duration::from_millis(40);

/// # Start

#[derive(Debug)]
pub struct Start {
    /// The index that the command will appear at if it's ever committed.
    pub index: usize,
    /// The current term.
    pub term: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("this node is not a leader, next leader: {0}")]
    NotLeader(usize),
    #[error("IO error")]
    IO(#[from] io::Error),
}

/// # State

#[derive(Clone, Debug)]
pub struct State {
    /// persistent:
    pub(crate) term: u64,
    /// vote in this term
    pub(crate) voted_for: Option<usize>,
    pub(crate) logs: Logs,

    /// volatile:
    pub(crate) commit_index: usize,
    pub(crate) applied_index: usize,

    /// leader only:
    pub(crate) next_index: Vec<usize>,
    pub(crate) match_index: Vec<usize>,
}

impl State {
    pub fn new(size: usize) -> Self {
        Self {
            term: 0,
            voted_for: None,
            logs: Logs::default(),
            commit_index: 1,
            applied_index: 1,
            next_index: vec![1; size],
            match_index: vec![0; size],
        }

    }
}


/// # Role

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}


/// # Raft

pub struct Raft {
    pub(crate) me: usize,
    pub(crate) role: Role,
    pub(crate) apply_ch: MsgSender,
    pub(crate) state: State,
    pub(crate) timer: Instant,
    pub(crate) common_sz: usize,
    /// Snapshot related data.
    /// Only leader maintains them to send snapshot to followers.
    pub(crate) last_included_log: Option<LogEntry>,
    pub(crate) snapshot: Vec<u8>,
}

impl Raft {
    pub(crate) fn start(&mut self, data: &[u8]) -> Result<Start> {
        if self.role != Role::Leader {
            let leader = (self.me + 1) % self.common_sz;
            return Err(Error::NotLeader(leader));
        }
        // is leader
        let term = self.state.term;
        let index = self.state.logs.end();
        self.state.logs.push(LogEntry { term, index, data: data.into() });
        Ok(Start{ term, index })
    }

    pub(crate) fn apply(&mut self) {
        if self.state.commit_index < self.state.logs.begin() {
            return;
        }
        if self.state.applied_index <= self.state.logs.begin() {
            let last_included_log = self.last_included_log.as_ref().unwrap();
            let msg = ApplyMsg::Snapshot {
                data: self.snapshot.clone(),
                term: last_included_log.term,
                index: last_included_log.index,
            };
            self.state.applied_index = self.state.logs.begin();
            self.apply_ch.unbounded_send(msg).unwrap();
        }
        while self.state.applied_index < self.state.commit_index {
            let index = self.state.applied_index;
            let log = self.state.logs[index].clone();
            let msg = ApplyMsg::Command {
                data: log.data,
                index: log.index,
            };
            self.state.applied_index += 1;
            self.apply_ch.unbounded_send(msg).unwrap();
        }
    }

    /// returns `last_log_term` and `last_log_index`
    fn last_log_info(&self) -> (u64, usize) {
        if let Some(log) = self.state.logs.last() {
            (log.term, log.index)
        } else if self.last_included_log.is_some() {
            // log is empty, but snapshot exists.
            let log = self.last_included_log.as_ref().unwrap();
            (log.term, log.index)
        } else {
            (0, 0)
        }
    }

    pub(crate) fn reset_state(&mut self, role: Role) {
        self.timer = Instant::now();
        match role {
            Role::Leader => {
                self.state.next_index.fill(self.state.logs.end());
                self.state.match_index.fill(self.state.logs.begin());
            },
            Role::Candidate => {
                self.state.term += 1;
                self.state.voted_for = Some(self.me);
            },
            Role::Follower => {},
        }
    }

    /// ## Follower Timer Functions

    pub(crate) fn get_timer(&self) -> Duration {
        self.timer.elapsed()
    }

    pub(crate) fn set_timer(&mut self) {
        self.timer = Instant::now()
    }

    /// ## Functions for different roles

    /// Send AppendEntry to one follower.
    ///
    /// Returns (Future sending AppendEntryArgs, new `next_index` value).
    pub(crate) fn send_append_entry(&self, id: usize, addr: SocketAddr)
        -> (impl Future<Output = RPCResult<AppendEntryReply>>, usize) {
        let new_next = self.state.logs.end();
        let args = {
            let prev_log_index = max(self.state.next_index[id], 1) - 1;
            let prev_log_term = if self.state.logs.contains_index(prev_log_index) {
                self.state.logs[prev_log_index].term
            } else {
                self.state.term
            };
            AppendEntryArgs {
                term: self.state.term,
                leader: self.me,
                prev_log_term,
                prev_log_index,
                log_entries: self.state.logs[self.state.next_index[id]..].into(),
                leader_commit_index: self.state.commit_index,
            }
        };
        let net = net::NetLocalHandle::current();
        (
            async move {
                net.call_timeout::<AppendEntryArgs, AppendEntryReply>(
                    addr, args, RPC_TIMEOUT,
                ).await
            }, new_next,
        )
    }

    /// For leaders to handle AppendEntryReply.
    ///
    /// Return values:
    /// - -1: role change to follower.
    /// - 0: success.
    /// - 1: retry AppendEntry.
    /// - 2: retry InstallSnapshot.
    pub(crate) fn handle_append_entry(
        &mut self, id: usize,
        reply: AppendEntryReply,
        new_next: usize,
        step: usize,
    ) -> i32 {
        if reply.success {
            // log entries match, update `next_index` and `match_index`.
            self.state.next_index[id] = new_next;
            self.state.match_index[id] = new_next;
            0
        } else if reply.term <= self.state.term {
            // log entries don't match, `next_index` retrieves.
            let begin = self.state.logs.begin();
            if self.state.next_index[id] == begin {
                2
            } else {
                self.state.next_index[id] = max(
                    self.state.next_index[id], 
                    max(begin, 1) + step
                ) - step;
                1
            }
        } else {
            self.role = Role::Follower;
            self.state.term = reply.term;
            self.state.voted_for = None;
            -1
        }
    }

    /// Send InstallSnapshot to one follower.
    ///
    /// Returns (Future sending InstallSnapshotArgs, `done`, `new_next`).
    pub(crate) fn send_install_snapshot(&self, addr: SocketAddr, offset: usize)
        -> (impl Future<Output = RPCResult<InstallSnapshotReply>>, bool, usize) {
        let new_offset = min(offset + SNAPSHOT_SIZE, self.snapshot.len());
        let done = new_offset == self.snapshot.len();
        let new_next;
        let args = {
            let last_include_log = self.last_included_log.as_ref().unwrap();
            new_next = last_include_log.index + 1;
            InstallSnapshotArgs {
                term: self.state.term,
                leader: self.me,
                last_included_term: last_include_log.term,
                last_included_index: last_include_log.index,
                offset,
                data: self.snapshot[offset..new_offset].to_owned(),
                done,
            }
        };
        let net = net::NetLocalHandle::current();
        (
            async move {
                net.call_timeout::<InstallSnapshotArgs, InstallSnapshotReply>(
                    addr, args, RPC_TIMEOUT
                ).await
            }, done, new_next,
        )
    }

    /// For leaders to handle InstallSnapshotReply.
    ///
    /// Return values:
    /// - -1: role change to follower.
    /// - 0: success.
    pub(crate) fn handle_install_snapshot(
        &mut self,
        reply: InstallSnapshotReply,
        id: usize,
        new_next: usize,
    ) -> i32 {
        if reply.term <= self.state.term {
            self.state.next_index[id] = new_next;
            self.state.match_index[id] = new_next;
            0
        } else {
            self.role = Role::Follower;
            self.state.term = reply.term;
            self.state.voted_for = None;
            -1
        }
    }

    /// For candidates to send RequestVoteArgs.
    pub(crate) fn send_request_vote(&self, peers: &Vec<SocketAddr>)
        -> FuturesUnordered<impl Future<Output = RPCResult<RequestVoteReply>>> {
        let net = net::NetLocalHandle::current();
        let rpcs = FuturesUnordered::new();
        let (last_log_term, last_log_index) = self.last_log_info();
        let args = RequestVoteArgs {
            term: self.state.term,
            candidate: self.me,
            last_log_term,
            last_log_index,
        };
        for &peer in peers.iter() {
            let net = net.clone();
            let args = args.clone();
            rpcs.push(async move {
                net.call_timeout::<RequestVoteArgs, RequestVoteReply>(
                    peer, args, RPC_TIMEOUT
                ).await
            });
        }
        rpcs
    }

    /// For candidates to handle RequestVoteReply.
    ///
    /// Return true if role changes to follower.
    /// Else stays candidate.
    pub(crate) fn handle_request_vote(&mut self, reply: RequestVoteReply) -> bool {
        if reply.term > self.state.term {
            info!("[{:?}] Receives reply from newer term, turns to follower.", self);
            self.role = Role::Follower;
            self.state.term = reply.term;
            self.state.voted_for = None;
            return true;
        }
        false
    }

    pub(crate) fn update_commit_index(&mut self) {
        let mut sorted_match = self.state.match_index.clone();
        sorted_match.sort();
        let mid = (self.common_sz + 1) >> 1;
        let commit_index = sorted_match[mid];
        if commit_index > self.state.commit_index {
            if self.state.logs.contains_index(commit_index - 1)
                && self.state.logs[commit_index - 1].term == self.state.term {
                self.state.commit_index = commit_index;
            }
        }
    }

    /// ## Reply requests

    /// Handle RequestVote RPC.
    ///
    /// Return true if turns to follower.
    pub(crate) fn on_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        // 1. newer term
        if args.term < self.state.term {
            return RequestVoteReply { term: self.state.term, vote_granted: false };
        }

        // If the candidate is in a newer term, turns to follower.
        if args.term > self.state.term {
            info!("[{:?}] Receive RequestVote from newer term, turn to follower.", self);
            self.role = Role::Follower;
            self.state.term = args.term;
            self.state.voted_for = None;
        }

        // reset follower timer
        self.set_timer();

        // 2. didn't vote in this term
        let not_voted = self.state.voted_for.is_none() || self.state.voted_for == Some(args.candidate);

        // 3. logs
        let (prev_term, prev_index) = self.last_log_info();
        let log_term_wins = args.last_log_term > prev_term;
        let log_index_wins = args.last_log_term == prev_term && args.last_log_index >= prev_index;

        let vote_granted= not_voted && (log_term_wins || log_index_wins);
        if vote_granted {
            self.state.voted_for = Some(args.candidate);
            self.role = Role::Follower;
        }
        // info!("[{:?}] on_request_vote: candidate={} (term={}), vote_granted={}; \n\t\t\t       not_voted {} ({:?}), log_term_wins {}, log_index_wins {}",
            // self, args.candidate, args.term, vote_granted, not_voted, self.state.voted_for, log_term_wins, log_index_wins);
        RequestVoteReply { term: self.state.term, vote_granted }
    }

    /// Handle AppendEntry RPC.
    ///
    /// Return true if turns to follower.
    pub(crate) fn on_append_entry(&mut self, mut args: AppendEntryArgs) -> AppendEntryReply {
        if args.term < self.state.term {
            return AppendEntryReply { term: self.state.term, success: false };
        }

        let success = args.prev_log_index + 1 == self.state.logs.begin()
            || self.state.logs.contains_index(args.prev_log_index)
            && self.state.logs[args.prev_log_index].term == args.prev_log_term;

        if args.term > self.state.term {
            self.role = Role::Follower;
            self.state.term = args.term;
            self.state.voted_for = None;
        }

        // reset follower timer
        self.set_timer();

        if !success {
            return AppendEntryReply { term: self.state.term, success };
        }

        self.role = Role::Follower;

        self.state.logs.trim_from(args.prev_log_index + 1);
        self.state.logs.append(&mut args.log_entries);
        
        // update commit_index
        if args.leader_commit_index > self.state.commit_index {
            let end = self.state.logs.end();
            self.state.commit_index = min(args.leader_commit_index, end);
            self.apply();
        }

        AppendEntryReply { term: self.state.term, success }
    }

    /// Handle AppendEntry RPC.
    ///
    /// Return true if turns to follower.
    pub(crate) fn on_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        let reply = InstallSnapshotReply { term: self.state.term };
        if self.state.term > args.term { return reply; }

        if args.term > self.state.term {
            self.role = Role::Follower;
            self.state.term = args.term;
            self.state.voted_for = None;
        }

        // reset follower timer
        self.set_timer();
        self.role = Role::Follower;

        self.last_included_log = Some(LogEntry {
            term: args.last_included_term,
            index: args.last_included_index,
            data: vec![],
        });
        if args.offset == 0 {
            self.snapshot = args.data;
        } else {
            let mut data = args.data;
            self.snapshot.append(&mut data);
        }
        self.state.logs.trim_to(args.last_included_index + 1);
        self.state.commit_index = args.last_included_index + 1;

        self.apply();
        reply
    }
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (T{}), {:?}", self.me, self.state.term, self.role)
    }
}


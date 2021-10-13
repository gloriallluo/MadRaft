use futures::{channel::mpsc, stream::FuturesUnordered, Future};
use madsim::{
    fs, net, task,
    rand::Rng,
    time::*,
};
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
    commit_index: usize,
    applied_index: usize,

    /// leader only:
    next_index: Vec<usize>,
    match_index: Vec<usize>,
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
    pub(crate) snapshot: Vec<u8>,
    pub(crate) common_sz: usize,
}

// HINT: put mutable non-async functions here
impl Raft {
    pub(crate) fn start(&mut self, data: &[u8]) -> Result<Start> {
        if self.role != Role::Leader {
            let leader = (self.me + 1) % self.common_sz;
            return Err(Error::NotLeader(leader));
        }
        // is leader
        let term = self.state.term;
        let index = if let Some(e) = self.state.logs.last() { e.index + 1 } else { 1 };
        // debug!("[{:?}] start: push (term {}, index {})", self, term, index);
        self.state.logs.push(LogEntry { term, index, data: data.into() });
        // debug!("Leader {} logs: {:?}", self.me, self.state.logs);
        Ok(Start{ term, index })
    }

    pub(crate) fn apply(&mut self) {
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
        } else {
            (self.state.term, 0)
        }
    }

    pub(crate) fn reset_state(&mut self, role: Role) {
        self.timer = Instant::now();
        match role {
            Role::Leader => {
                let first_index = self.state.logs.first_index().unwrap_or(self.state.logs.head());
                let last_index = self.state.logs.last_index().unwrap_or(self.state.logs.tail());
                self.state.next_index.fill(last_index);
                self.state.match_index.fill(first_index);
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

    pub(crate) fn send_append_entry(&self, id: usize, addr: SocketAddr)
        -> (impl Future<Output = RPCResult<AppendEntryReply>>, usize) {
        let new_next = self.state.logs.tail();
        let args = {
            // NOTE: `prev_log` is before the beginning of log entries sent to follower.
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
                net.call::<AppendEntryArgs, AppendEntryReply>(addr, args).await
            }, new_next
        )
    }

    /// For leaders to handle AppendEntryReply.
    ///
    /// Return 0: success; return 1: retry; return 2: role change to follower.
    pub(crate) fn handle_append_entry(
        &mut self, id: usize,
        reply: AppendEntryReply,
        new_next: usize,
        step: usize
    ) -> i32 {
        if reply.success {
            // log entries match, update `next_index` and `match_index`.
            self.state.next_index[id] = new_next;
            self.state.match_index[id] = new_next;
            0
        } else if reply.term <= self.state.term {
            // log entries don't match, `next_index` retrieves.
            if self.state.next_index[id] > step {
                self.state.next_index[id] -= step;
            } else {
                self.state.next_index[id] = 0;
            }
            1
        } else {
            self.role = Role::Follower;
            self.state.term = reply.term;
            self.state.voted_for = None;
            2
        }
    }

    pub(crate) fn send_install_snapshot(&self) {
        todo!("Send InstallSnapshotArgs")
    }

    pub(crate) fn handle_install_snapshot(&self) {
        todo!("Handle InstallSnapshotReply")
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
        // debug!("[{:?}] send_vote_request: {:?}", self, args);
        for &peer in peers.iter() {
            let net = net.clone();
            let args = args.clone();
            rpcs.push(async move {
                net.call::<RequestVoteArgs, RequestVoteReply>(peer, args).await
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
        if commit_index > self.state.commit_index
            && self.state.logs[commit_index - 1].term == self.state.term {
            self.state.commit_index = commit_index;
            // debug!("Leader commit_index updated: {}", commit_index);
        }
    }

    /// ## Reply requests

    /// Handle RequestVote RPC.
    ///
    /// Return true if turns to follower.
    pub(crate) fn on_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        // 1. newer term
        let newer = args.term >= self.state.term;
        // If the candidate is in a newer term, turns to follower.
        if args.term > self.state.term {
            // info!("[{:?}] Receive RequestVote from newer term, turn to follower.", self);
            self.role = Role::Follower;
            self.state.term = args.term;
            self.state.voted_for = None;
        }

        // 2. didn't vote in this term
        let not_voted = self.state.voted_for.is_none() || self.state.voted_for == Some(args.candidate);

        // 3. logs
        let (prev_term, prev_index) = self.last_log_info();
        // debug!("[{:?}] on_request_vote: my log term {}, my log index {}",
        //     self, prev_term, prev_index);
        let log_term_wins = args.last_log_term > prev_term;
        let log_index_wins = args.last_log_term == prev_term && args.last_log_index >= prev_index;

        let vote_granted= newer && not_voted && (log_term_wins || log_index_wins);
        if vote_granted {
            self.state.voted_for = Some(args.candidate);
            // reset follower timer
            self.set_timer();
        }

        // debug!("[{:?}] on_request_vote: candidate={} (term={}), vote_granted={}; \n\t\t\t       newer {}, not_voted {} ({:?}), log_term_wins {}, log_index_wins {}",
        //        self, args.candidate, args.term, vote_granted, newer, not_voted, self.state.voted_for, log_term_wins, log_index_wins);
        RequestVoteReply { term: self.state.term, vote_granted }
    }

    /// Handle AppendEntry RPC.
    ///
    /// Return true if turns to follower.
    pub(crate) fn on_append_entry(&mut self, args: AppendEntryArgs) -> AppendEntryReply {
        let prev_index = self.state.logs.last_index().unwrap_or(0);
        let success = args.term >= self.state.term
            && self.state.logs.contains_index(args.prev_log_index)
            && self.state.logs[args.prev_log_index].term == args.prev_log_term;
        // debug!("[{:?}] on_append_entry: logs from {} term {} ({}, {}), success = {}",
        //     self, args.leader, args.term, args.prev_log_term, args.prev_log_index, success);

        if args.term > self.state.term {
            self.role = Role::Follower;
            self.state.term = args.term;
            self.state.voted_for = None;
        }

        if !success {
            return AppendEntryReply { term: self.state.term, success };
        }
        // reset follower timer
        self.set_timer();

        for log in args.log_entries {
            if self.state.logs.len() > 0 && log.index <= prev_index {
                // conflict entries
                let index = log.index;
                self.state.logs[index] = log;
            } else {
                self.state.logs.push(log);
            }
        }
        // update commit_index
        if args.leader_commit_index > self.state.commit_index {
            self.state.commit_index = min(args.leader_commit_index, prev_index + 1);
            self.apply();
        }
        AppendEntryReply { term: self.state.term, success }
    }

    /// Handle AppendEntry RPC.
    ///
    /// Return true if turns to follower.
    pub(crate) fn on_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        debug!("[{:?}] on_install_snapshot", self);
        let reply = InstallSnapshotReply { term: self.state.term };
        if self.state.term > args.term { return reply }
        task::spawn(async move {
            let file = fs::File::create("snapshot").await.unwrap();
            file.write_all_at(&args.data, args.offset as u64).await
                .expect("File write_all_at failed");
            file.sync_all().await
                .expect("File sync_all failed");
        }).detach();
        reply
    }
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (T{}), {:?}", self.me, self.state.term, self.role)
    }
}


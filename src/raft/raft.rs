use futures::{channel::mpsc, stream::FuturesUnordered, StreamExt};
use madsim::{
    fs, net,
    rand::{self, Rng},
    task::{self, Task},
    time::*,
};
use std::{
    fmt, io,
    net::SocketAddr,
    sync::{Weak, Mutex},
    cmp::min
};
use crate::raft::{args::*, log::*};
use serde::{Deserialize, Serialize};


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


/// Start ///

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

pub type Result<T> = std::result::Result<T, Error>;


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
            commit_index: 0,
            applied_index: 0,
            next_index: vec![0; size],
            match_index: vec![0; size],
        }

    }
}


/// # Raft

pub struct Raft {
    pub(crate) peers: Vec<SocketAddr>,
    pub(crate) me: usize,
    pub(crate) apply_ch: MsgSender,
    pub(crate) role: Role,
    pub(crate) task: Vec<Task<()>>,
    pub(crate) state: State,
    pub(crate) myself: Weak<Mutex<Self>>,
    pub(crate) timer: Instant,
    pub(crate) snapshot: Vec<u8>,
}

// HINT: put mutable non-async functions here
impl Raft {
    pub(crate) fn start(&mut self, data: &[u8]) -> Result<Start> {
        if self.role != Role::Leader {
            let leader = (self.me + 1) % self.peers.len(); // ???
            return Err(Error::NotLeader(leader));
        }
        // is leader
        let term = self.state.term;
        let index = if let Some(e) = self.state.logs.last() { e.index + 1 } else { 0 };
        self.state.logs.push(LogEntry { term, index, data: data.into() });
        Ok(Start{ term, index })
    }

    fn apply(&mut self) {
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

    fn election_timeout() -> Duration {
        Duration::from_millis(rand::rng().gen_range(150..300))
    }

    fn heartbeat_timeout() -> Duration {
        Duration::from_millis(50)
    }

    fn reset_state(&mut self, role: Role) {
        self.task.clear();
        self.timer = Instant::now();
        match role {
            Role::Leader => {
                self.role = Role::Leader;
                let first_index = self.state.logs.first_index().unwrap_or(0);
                let last_index = self.state.logs.last_index().unwrap_or(self.state.logs.len());
                self.state.next_index.fill(last_index);
                self.state.match_index.fill(first_index);
            },
            Role::Candidate => {
                self.role = Role::Candidate;
                self.state.term += 1;
                self.state.voted_for = Some(self.me);
            },
            Role::Follower => {
                self.role = Role::Follower;
            },
        }
    }

    /// ## Functions for different roles

    pub(crate) fn run_leader(&mut self) {
        self.reset_state(Role::Leader);
        debug!("[{:?}] run_leader, term={:?}", self, self.state.term);

        // 1. Send heartbeat AppendEntry RPC to followers.
        let heartbeat_interval = Self::heartbeat_timeout();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me { continue; }
            let net = net::NetLocalHandle::current();
            let ptr = self.myself.clone();
            self.task.push(task::spawn(async move {
                loop {
                    loop {
                        // Send AppendEntry to one of follower, until logs match.
                        let step = 1;
                        let new_next: usize;
                        let args = {
                            let ptr = ptr.upgrade().unwrap();
                            let raft = ptr.lock().unwrap();
                            new_next = raft.state.logs.len();
                            // NOTE: `prev_log` is the beginning of log entries sent to follower.
                            let prev_log_index = raft.state.next_index[i];
                            let prev_log_term = if raft.state.logs.contains_index(prev_log_index) {
                                raft.state.logs[prev_log_index].term
                            } else {
                                raft.state.term
                            };
                            AppendEntryArgs {
                                term: raft.state.term,
                                leader: raft.me,
                                prev_log_term,
                                prev_log_index,
                                log_entries: raft.state.logs[raft.state.next_index[i]..].into(),
                                leader_commit_index: raft.state.commit_index,
                            }
                        };  // lock dropped
                        let reply = net.call::<AppendEntryArgs, AppendEntryReply>(peer, args)
                            .await.unwrap();
                        let ptr = ptr.upgrade().unwrap();
                        let mut raft = ptr.lock().unwrap();
                        if reply.success {
                            // log entries match, update `next_index` and `match_index`.
                            raft.state.next_index[i] = new_next;
                            raft.state.match_index[i] = new_next;
                            break;
                        } else {
                            // log entries don't match, `next_index` retrieves.
                            raft.state.next_index[i] -= step;
                        }
                    }   // loop rpc
                    sleep(heartbeat_interval).await;
                }   // loop heartbeat
            }));
        }   // for

        // 2. Update `commit_index` and apply.
        let ptr = self.myself.clone();
        self.task.push(task::spawn(async move {
            loop {
                sleep(Duration::from_millis(40)).await;
                // update `commit_index` due to `match_index`, and apply logs.
                let ptr = ptr.upgrade().unwrap();
                let mut raft = ptr.lock().unwrap();
                raft.update_commit_index();
                raft.apply();
            }
        }));
    }

    pub(crate) fn run_candidate(&mut self) {
        self.reset_state(Role::Candidate);
        debug!("[{:?}] run_candidate, term={}", self, self.state.term);

        // 1. Send RequestVote RPC.
        let net = net::NetLocalHandle::current();
        let mut rpcs = FuturesUnordered::new();
        let (last_log_term, last_log_index) = self.last_log_info();
        let args = RequestVoteArgs {
            term: self.state.term,
            candidate: self.me,
            last_log_term,
            last_log_index,
        };
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me { continue; }
            let net = net.clone();
            let args = args.clone();
            rpcs.push(async move {
                net.call::<RequestVoteArgs, RequestVoteReply>(peer, args).await
            });
        }

        // 2. Collect votes.
        let majority = (self.peers.len() + 1) >> 1;
        let ptr = self.myself.clone();
        self.task.push(task::spawn(async move {
            let mut count = 1;
            while let Some(reply) = rpcs.next().await {
                let ptr = ptr.upgrade().unwrap();
                let mut raft = ptr.lock().unwrap();
                let reply = reply.unwrap();
                // update term
                if reply.term > raft.state.term {
                    info!("[{:?}] Receives reply from newer term, turns to follower.", raft);
                    raft.state.term = reply.term;
                    raft.state.voted_for = None;
                    raft.run_follower();
                }
                if reply.vote_granted { count += 1; }
                if count >= majority {
                    info!("[{:?}] Wins election, turns to leader.", raft);
                    raft.run_leader();
                }
            }
            // lose election
            let ptr = ptr.upgrade().unwrap();
            let mut raft = ptr.lock().unwrap();
            info!("[{:?}] Loses election, turn to follower.", raft);
            raft.state.voted_for = None;
            raft.run_follower();
        }));

        // 3. If timeout, start another election.
        let ptr = self.myself.clone();
        self.task.push(task::spawn(async move {
            sleep(Self::election_timeout()).await;
            let ptr = ptr.upgrade().unwrap();
            let mut raft = ptr.lock().unwrap();
            info!("[{:?}] Election timeout, start a new election.", raft);
            raft.run_candidate();
        }));
    }

    pub(crate) fn run_follower(&mut self) {
        self.reset_state(Role::Follower);
        debug!("[{:?}] run_follower", self);

        // 1. Start an election when timeout.
        let timeout = Self::election_timeout();
        let ptr = self.myself.clone();
        self.task.push(task::spawn(async move {
            loop {
                sleep(Duration::from_millis(20)).await;
                let ptr = ptr.upgrade().unwrap();
                let mut raft = ptr.lock().unwrap();
                if raft.timer.elapsed() > timeout {
                    info!("[{:?}] Election timeout, turns to Candidate.", raft);
                    raft.run_candidate();
                }
            }
        }));
    }

    fn update_commit_index(&mut self) {
        assert_eq!(self.role, Role::Leader);
        let mut sorted_match = self.state.match_index.clone();
        sorted_match.sort();
        let mid = (self.peers.len() + 1) >> 1;
        let commit_index = sorted_match[mid];
        if commit_index > self.state.commit_index
            && self.state.logs[commit_index - 1].term == self.state.term {
            self.state.commit_index = commit_index;
        }
    }

    /// ## Reply requests

    pub(crate) fn on_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let (prev_term, prev_index) = self.last_log_info();
        // vote granted if:
        // (1) candidate term is not stale;
        // (2) haven't vote in this term or have voted for the specific candidate;
        // (3) candidate's log is better than mine.
        let newer = args.term >= self.state.term;
        let not_voted = self.state.voted_for.is_none() || self.state.voted_for == Some(args.candidate);
        let log_term_wins = args.last_log_term > prev_term;
        let log_index_wins = args.last_log_term == prev_term && args.last_log_index >= prev_index;
        let vote_granted= newer && not_voted && (log_term_wins || log_index_wins);
        if vote_granted { self.state.voted_for = Some(args.candidate); }
        // If the candidate is in a newer term, turns to follower.
        if args.term > self.state.term && self.role != Role::Follower {
            info!("[{:?}] Receive RequestVote from newer term, turn to follower.", self);
            self.run_follower();
        }
        debug!("[{:?}] on_request_vote, candidate={} (term={}), vote_granted={}; \n\t\t\t\tnewer {}, not_voted {} ({:?}), log_term_wins {}, log_index_wins {}",
            self, args.candidate, args.term, vote_granted, newer, not_voted, self.state.voted_for, log_term_wins, log_index_wins);
        RequestVoteReply { term: self.state.term, vote_granted }
    }

    pub(crate) fn on_append_entry(&mut self, args: AppendEntryArgs) -> AppendEntryReply {
        // debug!("[{:?}] on_append_entry", self);
        let prev_index = self.state.logs.last_index().unwrap_or(0);
        let success = args.term >= self.state.term
            && (args.prev_log_index == 0
            || self.state.logs.contains_index(args.prev_log_index - 1)
            && self.state.logs[args.prev_log_index - 1].term == args.prev_log_term);
        // new leader is elected
        if args.term > self.state.term {
            self.state.term = args.term;
            self.state.voted_for = None;
        }
        if !success {
            return AppendEntryReply { term: self.state.term, success };
        }
        // reset follower timer
        self.timer = Instant::now();
        // update commit_index
        if args.leader_commit_index > self.state.commit_index {
            self.state.commit_index = min(args.leader_commit_index, prev_index + 1);
            self.apply();
        }
        for log in args.log_entries {
            if self.state.logs.len() > 0 && log.index <= prev_index {
                // conflict entries
                let index = log.index;
                self.state.logs[index] = log;
            } else {
                self.state.logs.push(log);
            }
        }
        if self.role != Role::Follower {
            info!("[{:?}] Receive AppendEntry, turn to follower.", self);
            self.run_follower();
        }
        AppendEntryReply { term: self.state.term, success }
    }

    pub(crate) fn on_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        debug!("[{:?}] on_install_snapshot", self);
        let reply = InstallSnapshotReply { term: self.state.term };
        if self.state.term > args.term { return reply; }
        self.task.push(task::spawn(async move {
            let file = fs::File::create("snapshot").await.unwrap();
            file.write_all_at(&args.data, args.offset as u64).await
                .expect("File write_all_at failed");
            file.sync_all().await
                .expect("File sync_all failed");
        }));
        reply
    }
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}: {:?}>, term {}",
               self.me, self.role, self.state.term)
    }
}



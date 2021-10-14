use std::{
    io, fmt,
    sync::{Arc, Mutex},
    net::SocketAddr,
};
use futures::{
    channel::mpsc,
    stream::FuturesUnordered,
    StreamExt,
    FutureExt,
    select_biased,
    pin_mut,
};
use crate::raft::{raft::*, args::*, log::*};
use madsim::{time::*, fs, net, task, rand::{self, Rng}};
use serde::{Deserialize, Serialize};


/// Things stored in persistent storage:
/// State data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    term: u64,
    voted_for: Option<usize>,
    logs: Vec<LogEntry>,
}

/// Snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    data: Vec<u8>,
}


/// # Results

pub type Result<T> = std::result::Result<T, Error>;
pub type RPCResult<T> = std::io::Result<T>;


/// # RaftHandle

#[derive(Clone)]
pub struct RaftHandle {
    me: usize,
    peers: Vec<SocketAddr>,
    inner: Arc<Mutex<Raft>>,
}

impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let size = peers.len();
        let inner = Arc::new(Mutex::new(Raft {
            me,
            role: Role::default(),
            apply_ch,
            state: State::new(size),
            timer: Instant::now(),
            common_sz: peers.len(),
            last_included_log: None,
            snapshot: Vec::new(),
        }));
        let handle = RaftHandle {
            me,
            peers,
            inner
        };
        // initialize from state persisted before a crash
        handle.restore().await.expect("Failed to restore");
        handle.start_rpc_server();
        let me = handle.clone();
        task::spawn(async move { me.run().await }).detach();
        (handle, recver)
    }

    /// Start agreement on the next command to be appended to Raft's log.
    ///
    /// If this server isn't the leader, returns [`Error::NotLeader`].
    /// Otherwise start the agreement and return immediately.
    ///
    /// There is no guarantee that this command will ever be committed to the
    /// Raft log, since the leader may fail or lose an election.
    pub async fn start(&self, cmd: &[u8]) -> Result<Start> {
        let mut raft = self.inner.lock().unwrap();
        raft.start(cmd)
    }

    async fn run(&self) {
        loop {
            match self.get_role() {
                Role::Leader => {
                    self.inner.lock().unwrap().reset_state(Role::Leader);
                    self.run_leader().await;
                },
                Role::Candidate => {
                    self.inner.lock().unwrap().reset_state(Role::Candidate);
                    self.run_candidate().await;
                },
                Role::Follower => {
                    self.inner.lock().unwrap().reset_state(Role::Follower);
                    self.run_follower().await;
                },
            };
        }
    }

    fn get_role(&self) -> Role {
        self.inner.lock().unwrap().role
    }

    /// ## leader functions

    async fn run_leader(&self) {
        let mut heartbeat_tasks = FuturesUnordered::new();
        self.peers
            .iter()
            .enumerate()
            .filter(|(i, _)| i != &self.me)
            .for_each(|(i, _)| {
                heartbeat_tasks.push(self.heartbeat(i));
            });
        let apply_task = self.apply().fuse();
        let persist_task = self.leader_persist().fuse();
        pin_mut!(apply_task, persist_task);
        loop {
            select_biased! {
                () = heartbeat_tasks.select_next_some() => return,
                () = apply_task => {},
                () = persist_task => {},
                complete => {},
            }
        }
    }

    /// Send AppendEntry or InstallSnapshot to followers
    async fn heartbeat(&self, id: usize) {
        loop {
            // Send heartbeat messages to follower every 50 milliseconds.
            let mut step: usize = 1;
            let mut offset: usize = 0;
            let mut append_entry = true;
            loop {
                if append_entry {
                    // Try until logs match
                    let (rpc, new_next) = {
                        let inner = self.inner.lock().unwrap();
                        if inner.role != Role::Leader { return; }
                        if inner.state.next_index[id] < inner.state.logs.begin() {
                            append_entry = false;
                            continue;
                        }
                        // debug!("[{}] heartbeat: append entry to {}, logs from {}, leader log {:?}",
                        //     self.me, id, inner.state.next_index[id], inner.state.logs);
                        inner.send_append_entry(id, self.peers[id])
                    };

                    let timeout = sleep(Self::rpc_timeout()).fuse();
                    let rpc = rpc.fuse();
                    pin_mut!(rpc, timeout);
                    select_biased! {
                        _ = timeout => continue,
                        reply = rpc => {
                            match reply {
                                Ok(reply) => {
                                    let mut inner = self.inner.lock().unwrap();
                                    match inner.handle_append_entry(id, reply, new_next, step) {
                                        // AppendEntry accepted.
                                        0 => break,
                                        // retry due to log mismatch.
                                        1 => { step <<= 1; continue; },
                                        // follower lag too much, send snapshot.
                                        2 => { append_entry = false; continue; },
                                        // turn follower
                                        -1 => return,
                                        _ => {},
                                    }
                                },
                                Err(_) => continue,
                            }
                        },
                    }
                } else {
                    // Send snapshot to follower.
                    let (rpc, done, new_next) = {
                        let inner = self.inner.lock().unwrap();
                        if inner.role != Role::Leader { return; }
                        inner.send_install_snapshot(self.peers[id], offset)
                    };

                    let timeout = sleep(Self::rpc_timeout()).fuse();
                    let rpc = rpc.fuse();
                    pin_mut!(rpc, timeout);
                    select_biased! {
                        _ = timeout => continue,
                        reply = rpc => {
                            match reply {
                                Ok(reply) => {
                                    let mut inner = self.inner.lock().unwrap();
                                    match inner.handle_install_snapshot(reply, id, new_next) {
                                        // InstallSnapshot accepted.
                                        0 => {
                                            if done {
                                                break;
                                            } else {
                                                offset += SNAPSHOT_SIZE;
                                                continue;
                                            }
                                        },
                                        // turn follower
                                        -1 => return,
                                        _ => {},
                                    }
                                },
                                Err(_) => continue,
                            }
                        },
                    }
                }
            } // loop retry
            // Heartbeat interval
            sleep(Self::heartbeat_timeout()).await;
        }
    }

    async fn apply(&self) {
        loop {
            // Update commit_index every 100 milliseconds.
            sleep(Duration::from_millis(100)).await;
            // update `commit_index` due to `match_index`, and apply logs.
            let mut inner = self.inner.lock().unwrap();
            inner.update_commit_index();
            inner.apply();
        }
    }

    async fn leader_persist(&self) {
        loop {
            // persist every 80 milliseconds
            sleep(Duration::from_millis(80)).await;
            self.persist().await;
        }
    }

    /// ## Candidate functions

    async fn run_candidate(&self) {
        let vote_request = self.vote_request().fuse();
        let timer = self.candidate_timer().fuse();
        pin_mut!(vote_request, timer);
        select_biased! {
            () = vote_request => return,
            () = timer => return,
        }
    }

    async fn vote_request(&self) {
        // send RequestVote
        let peer_without_me = self.peers
            .iter()
            .enumerate()
            .filter(|(i, _)| i != &self.me)
            .map(|(_, &addr)| addr)
            .collect();
        let mut rpcs = {
            let inner = self.inner.lock().unwrap();
            inner.send_request_vote(&peer_without_me)
        };

        if self.get_role() != Role::Candidate { return; }

        // collect votes
        let majority = (self.peers.len() + 1) >> 1;
        let mut count = 1;
        loop {
            select_biased! {
                reply = rpcs.select_next_some() => {
                    // info!("got reply: {:?}", reply);
                    let reply = reply.unwrap();
                    if reply.vote_granted { count += 1; }
                    let mut inner = self.inner.lock().unwrap();
                    if inner.handle_request_vote(reply) { return; }
                    if count >= majority && inner.role == Role::Candidate {
                        info!("[{}] Wins election, turns to leader.", self.me);
                        inner.role = Role::Leader;
                        return;
                    }
                },
                complete => break,
            }
        }
        // lose election
        let mut inner = self.inner.lock().unwrap();
        info!("[{:?}] Loses election, turn to follower.", inner);
        inner.role = Role::Follower;
        inner.state.voted_for = None;
    }

    async fn candidate_timer(&self) {
        sleep(Self::election_timeout()).await;
    }

    /// ## Follower functions

    async fn run_follower(&self) {
        let timer = self.follower_timer().fuse();
        pin_mut!(timer);
        select_biased! {
            () = timer => return,
        }
    }

    async fn follower_timer(&self) {
        loop {
            let timeout = Self::election_timeout();
            let deadline = {
                let inner = self.inner.lock().unwrap();
                inner.timer + timeout
            };
            sleep_until(deadline).await;
            let inner = self.inner.lock().unwrap();
            if inner.get_timer() >= timeout {
                info!("[{:?}] Election timeout, turns to Candidate. term = {}, log = {:?}",
                      inner, inner.state.term, inner.state.logs);
                let mut inner = inner;
                inner.role = Role::Candidate;
                return;
            }
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_role() == Role::Leader
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and include) that index. Raft should now trim its log as much as possible.
    pub async fn snapshot(&self, index: usize, snapshot: &[u8]) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            assert!(index < inner.state.commit_index);
            debug!("[{:?}] snapshot index {}, my commit_index {}",
                inner, index, inner.state.commit_index);
            inner.snapshot = snapshot.into();
            inner.last_included_log = Some(inner.state.logs[index].to_owned());
            inner.state.logs.trim(index);
            debug!("[{:?}] logs after trim: {:?}", inner, inner.state.logs);
        }
        self.persist().await?;
        Ok(())
    }

    /// Save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    ///
    /// See paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self) -> io::Result<()> {
        let (state, snapshot) = {
            let inner = self.inner.lock().unwrap();
            // debug!("[{:?}] logs {:?} persisted", self, inner.state.logs);
            let persist: Persist = Persist {
                term: inner.state.term,
                voted_for: inner.state.voted_for,
                logs: inner.state.logs.clone().into(),
            };
            let state = bincode::serialize(&persist).unwrap();
            let snapshot: Vec<u8> = inner.snapshot.clone();
            (state, snapshot)
        };

        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        file.sync_all().await?;

        let file = fs::File::create("snapshot").await?;
        file.write_all_at(&snapshot, 0).await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        match fs::read("snapshot").await {
            Ok(snapshot) => if snapshot.len() > 0 {
                // FIXME: broken snapshot
                let snapshot: Snapshot = bincode::deserialize(&snapshot).unwrap();
                let mut inner = self.inner.lock().unwrap();
                inner.snapshot = snapshot.data;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                let mut inner = self.inner.lock().unwrap();
                inner.state.term = persist.term;
                inner.state.voted_for = persist.voted_for;
                inner.state.logs = Logs::from(persist.logs);
                debug!("[{:?}] logs {:?} restored", self, inner.state.logs);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    /// Add some RPC handlers.
    fn start_rpc_server(&self) {
        let net = net::NetLocalHandle::current();
        let this = self.clone();
        net.add_rpc_handler(move |args: RequestVoteArgs| {
            let this = this.clone();
            async move { this.on_request_vote(args).await.unwrap() }
        });

        let this = self.clone();
        net.add_rpc_handler(move |args: AppendEntryArgs| {
            let this = this.clone();
            async move { this.on_append_entry(args).await.unwrap() }
        });

        let this = self.clone();
        net.add_rpc_handler(move |args: InstallSnapshotArgs| {
            let this = this.clone();
            async move { this.on_install_snapshot(args).await.unwrap() }
        });
    }

    async fn on_request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.on_request_vote(args)
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        let this = self.clone();
        task::spawn(async move { this.persist().await }).detach();
        Ok(reply)
    }

    async fn on_append_entry(&self, args: AppendEntryArgs) -> Result<AppendEntryReply> {
        // debug!("[{}] on_append_entry", self.me);
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.on_append_entry(args)
        };
        let this = self.clone();
        task::spawn(async move { this.persist().await }).detach();
        Ok(reply)
    }

    async fn on_install_snapshot(&self, args: InstallSnapshotArgs) -> Result<InstallSnapshotReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.on_install_snapshot(args)
        };
        let this = self.clone();
        task::spawn(async move { this.persist().await }).detach();
        Ok(reply)
    }

    /// A service wants to switch to snapshot.
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub async fn cond_install_snapshot(
        &self,
        _last_included_term: u64,
        last_included_index: u64,
        _snapshot: &[u8],
    ) -> bool {
        let inner = self.inner.lock().unwrap();
        let last_included_index = last_included_index as usize;
        inner.last_included_log.is_some() && last_included_index < inner.state.commit_index
    }

    fn election_timeout() -> Duration {
        Duration::from_millis(rand::rng().gen_range(150..300))
    }

    fn heartbeat_timeout() -> Duration {
        Duration::from_millis(50)
    }

    fn rpc_timeout() -> Duration {
        Duration::from_millis(50)
    }
}

impl fmt::Debug for RaftHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>", self.me)
    }
}
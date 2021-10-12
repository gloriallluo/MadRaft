use std::{
    io, fmt, result,
    sync::{Arc, Mutex, Weak},
    net::SocketAddr,
};
use crate::raft::{
    raft::*,
    args::*,
    log::*,
};
use futures::{
    channel::mpsc,
    stream::FuturesUnordered,
    StreamExt,
    FutureExt,
    select_biased,
    pin_mut,
};
use madsim::{time::*, fs, net, task, rand::{self, Rng}};
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;


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
enum HandleResult {
    Continue,
    RoleChange(Role),
}

/// # RaftHandle

#[derive(Clone)]
pub struct RaftHandle {
    me: usize,
    role: Arc<Mutex<Role>>,
    peers: Vec<SocketAddr>,
    inner: Arc<Mutex<Raft>>,
}

impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let size = peers.len();
        let inner = Arc::new(Mutex::new(Raft {
            me,
            apply_ch,
            state: State::new(size),
            timer: Instant::now(),
            snapshot: Vec::new(),
            common_sz: peers.len(),
        }));
        let handle = RaftHandle {
            me,
            role: Arc::new(Mutex::new(Role::Follower)),
            peers,
            inner
        };
        // initialize from state persisted before a crash
        handle.restore().await.expect("Failed to restore");
        handle.start_rpc_server();
        let mut me = handle.clone();
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
        info!("{:?} start", *raft);
        raft.start(cmd)
    }

    async fn run(&self) {
        loop {
            // debug!("[{}] run", self.me);
            let new_role = match self.get_role() {
                Role::Leader => {
                    self.inner.lock().unwrap().reset_state(Role::Leader);
                    self.run_leader().await
                },
                Role::Candidate => {
                    self.inner.lock().unwrap().reset_state(Role::Candidate);
                    self.run_candidate().await
                },
                Role::Follower => {
                    self.inner.lock().unwrap().reset_state(Role::Follower);
                    self.run_follower().await
                },
            };
            self.set_role(new_role);
        }
    }

    fn get_role(&self) -> Role {
        *self.role.lock().unwrap()
    }

    fn set_role(&self, role: Role) {
        *self.role.lock().unwrap() = role;
    }

    /// ## leader functions

    async fn run_leader(&self) -> Role {
        let mut heartbeat_tasks = FuturesUnordered::new();
        self.peers
            .iter()
            .enumerate()
            .filter(|(i, _)| i != &self.me)
            .for_each(|(i, _)| {
                heartbeat_tasks.push(self.heartbeat(i));
            });
        let apply_task = self.apply().fuse();
        pin_mut!(apply_task);
        loop {
            select_biased! {
                res = heartbeat_tasks.select_next_some() => match res {
                    HandleResult::Continue => {},
                    HandleResult::RoleChange(role) => return role,
                },
                () = apply_task => {},
                complete => {},
            }
        }
    }

    /// Send AppendEntry or InstallSnapshot to followers
    async fn heartbeat(&self, id: usize) -> HandleResult {
        loop {
            // Send heartbeat messages to follower every 50 milliseconds.
            let role = self.get_role();
            // debug!("[{}] my role is {:?}", self.me, role);
            if role != Role::Leader {
                // debug!("[{}] hb({}) role change!", self.me, id);
                return HandleResult::RoleChange(role);
            }
            // debug!("[{}] send heartbeat to {}", self.me, id);
            loop {
                // Try until logs match
                let (rpc, new_next) = {
                    let inner = self.inner.lock().unwrap();
                    inner.send_append_entry(id, self.peers[id])
                };
                let timeout = sleep(Self::rpc_timeout()).fuse();
                let rpc = rpc.fuse();
                pin_mut!(rpc, timeout);
                select_biased! {
                    _ = timeout => break,
                    reply = rpc => {
                        match reply {
                            Ok(reply) => {
                                let mut inner = self.inner.lock().unwrap();
                                if inner.handle_append_entry(id, reply, new_next) { break; }
                            },
                            Err(_) => break,
                        }
                    },
                }
            }
            // Heartbeat interval
            sleep(Self::heartbeat_timeout()).await;
        }
    }

    async fn apply(&self) {
        loop {
            // Update commit_index every 100 milli seconds.
            sleep(Duration::from_millis(100)).await;
            // update `commit_index` due to `match_index`, and apply logs.
            let mut inner = self.inner.lock().unwrap();
            inner.update_commit_index();
            inner.apply();
        }
    }

    /// ## Candidate functions

    async fn run_candidate(&self) -> Role {
        let vote_request = self.vote_request().fuse();
        let timer = self.candidate_timer().fuse();
        pin_mut!(vote_request, timer);
        loop {
            select_biased! {
                res = vote_request => match res {
                    HandleResult::Continue => {},
                    HandleResult::RoleChange(role) => return role,
                },
                res = timer => match res {
                    HandleResult::Continue => {},
                    HandleResult::RoleChange(role) => return role,
                },
            }
        }
    }

    async fn vote_request(&self) -> HandleResult {
        // send RequestVote
        let peer_without_me = self.peers
            .iter()
            .enumerate()
            .filter(|(i, _)| i != &self.me)
            .map(|(i, &addr)| addr)
            .collect();
        let mut rpcs = {
            let inner = self.inner.lock().unwrap();
            inner.send_request_vote(&peer_without_me)
        };

        // collect votes
        let majority = (self.peers.len() + 1) >> 1;
        let mut count = 1;
        while let Some(reply) = rpcs.next().await {
            let reply = reply.unwrap();
            if reply.vote_granted { count += 1; }
            let mut inner = self.inner.lock().unwrap();
            if inner.handle_request_vote(reply) {
                return HandleResult::RoleChange(Role::Follower);
            }
            if count >= majority && self.get_role() == Role::Candidate {
                info!("[{}] Wins election, turns to leader.", self.me);
                return HandleResult::RoleChange(Role::Leader);
            }
        }
        // lose election
        let mut inner = self.inner.lock().unwrap();
        info!("[{}] Loses election, turn to follower.", self.me);
        inner.state.voted_for = None;
        HandleResult::RoleChange(Role::Follower)
    }

    async fn candidate_timer(&self) -> HandleResult {
        sleep(Self::election_timeout()).await;
        HandleResult::RoleChange(Role::Candidate)
    }

    /// ## Follower functions

    async fn run_follower(&self) -> Role {
        let timer = self.follower_timer().fuse();
        pin_mut!(timer);
        select_biased! {
            res = timer => match res {
                HandleResult::Continue => unreachable!(),
                HandleResult::RoleChange(role) => return role,
            },
        }
    }

    async fn follower_timer(&self) ->HandleResult {
        loop {
            let timeout = Self::election_timeout();
            sleep(timeout).await;
            let inner = self.inner.lock().unwrap();
            if inner.get_timer() > timeout {
                info!("[{}] Election timeout, turns to Candidate.", self.me);
                return HandleResult::RoleChange(Role::Candidate);
            }
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.inner.lock().unwrap();
        raft.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        *self.role.lock().unwrap() == Role::Leader
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and include) that index. Raft should now trim its log as much as possible.
    pub async fn snapshot(&self, index: usize, snapshot: &[u8]) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.snapshot = snapshot.into();
            inner.state.logs.trim(index);
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
            Ok(snapshot) => {
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
        let (reply, to_follower) = {
            let mut this = self.inner.lock().unwrap();
            this.on_request_vote(args)
        };
        if to_follower { self.set_role(Role::Follower); }
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        self.persist().await.expect("Failed to persist");
        Ok(reply)
    }

    async fn on_append_entry(&self, args: AppendEntryArgs) -> Result<AppendEntryReply> {
        // debug!("[{}] on_append_entry", self.me);
        let (reply, to_follower) = {
            let mut this = self.inner.lock().unwrap();
            this.on_append_entry(args)
        };
        if to_follower { self.set_role(Role::Follower); }
        Ok(reply)
    }

    async fn on_install_snapshot(&self, args: InstallSnapshotArgs) -> Result<InstallSnapshotReply> {
        let (reply, _) = {
            let mut this = self.inner.lock().unwrap();
            this.on_install_snapshot(args)
        };
        Ok(reply)
    }

    /// A service wants to switch to snapshot.
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub async fn cond_install_snapshot(
        &self,
        _last_included_term: u64,
        _last_included_index: u64,
        _snapshot: &[u8],
    ) -> bool {
        true
    }

    fn election_timeout() -> Duration {
        Duration::from_millis(rand::rng().gen_range(150..300))
    }

    fn heartbeat_timeout() -> Duration {
        Duration::from_millis(50)
    }

    fn rpc_timeout() -> Duration {
        Duration::from_millis(20)
    }
}

impl fmt::Debug for RaftHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}: {:?}>", self.me, self.role)
    }
}
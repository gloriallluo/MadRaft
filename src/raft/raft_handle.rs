use std::{fmt, io, net::SocketAddr, sync::{Arc, Mutex}};
use futures::{
    FutureExt, 
    StreamExt, 
    channel::mpsc, 
    pin_mut, 
    select_biased, 
    join, 
    stream::FuturesUnordered, 
};
use crate::raft::{raft::*, args::*, log::*};
use madsim::{time::*, fs, net, task, rand::{self, Rng}};
use serde::{Deserialize, Serialize};


/// Things stored in persistent storage:
/// State data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    /// Raft state:
    term: u64,
    voted_for: Option<usize>,
    logs: Logs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    snapshot: Vec<u8>,
    last_included_log: Option<LogEntry>,
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
            leader: None,
            common_sz: peers.len(),
            last_included_log: None,
            snapshot: Vec::new(),
            snapshot_done: true,
            log_size: 0,
        }));
        let handle = RaftHandle {
            me,
            peers,
            inner,
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
            self.inner.lock().unwrap().reset_raft();
            match self.role() {
                Role::Leader => self.run_leader().await,
                Role::Candidate => self.run_candidate().await,
                Role::Follower => self.run_follower().await,
            };
        }
    }

    fn role(&self) -> Role {
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
        pin_mut!(apply_task);
        loop {
            select_biased! {
                _ = heartbeat_tasks.select_next_some() => return,
                _ = apply_task => {},
                complete => unreachable!(),
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
                        inner.send_append_entry(id, self.peers[id])
                    };

                    let rpc = rpc.fuse();
                    let persist = self.persist().fuse();
                    pin_mut!(rpc, persist);
                    let mut complete = false;
                    loop {
                        select_biased! {
                            _ = persist => continue,
                            reply = rpc => {
                                match reply {
                                    Ok(reply) => {
                                        let mut inner = self.inner.lock().unwrap();
                                        match inner.handle_append_entry(id, reply, new_next, step) {
                                            // AppendEntry accepted.
                                            0 => { complete = true; break; },
                                            // retry due to log mismatch.
                                            1 => { step <<= 1; break; },
                                            // follower lag too much, send snapshot.
                                            2 => { append_entry = false; break; },
                                            // turn follower
                                            -1 => return,
                                            _ => {},
                                        }
                                    },
                                    // rpc timeout
                                    Err(_) => break,
                                }
                            },
                        }
                    } // loop select
                    if complete { break; }
                } else {
                    // Send snapshot to follower.
                    let (rpc, done, new_next) = {
                        let inner = self.inner.lock().unwrap();
                        if inner.role != Role::Leader { return; }
                        inner.send_install_snapshot(self.peers[id], offset)
                    };

                    let rpc = rpc.fuse();
                    let persist = self.persist().fuse();
                    pin_mut!(rpc, persist);
                    let mut complete = false;
                    loop {
                        select_biased! {
                            _ = persist => continue,
                            reply = rpc => {
                                match reply {
                                    Ok(reply) => {
                                        let mut inner = self.inner.lock().unwrap();
                                        match inner.handle_install_snapshot(reply, id, new_next) {
                                            // InstallSnapshot accepted.
                                            0 => {
                                                if done { 
                                                    complete = true; 
                                                } else { 
                                                    offset += SNAPSHOT_SIZE; 
                                                }
                                                break;
                                            },
                                            // turn follower
                                            -1 => return,
                                            _ => {},
                                        }
                                    },
                                    // rpc timeout
                                    Err(_) => break,
                                }
                            },
                        }
                    } // loop select
                    if complete { break; }
                }
            } // loop retry
            // Heartbeat interval
            sleep(Self::heartbeat_timeout()).await;
        } // loop heartbeat
    }

    async fn apply(&self) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.update_commit_index(false);
            inner.apply();
        }
        loop {
            // Update commit_index every 100 milliseconds.
            sleep(Duration::from_millis(100)).await;
            // update `commit_index` due to `match_index`, and apply logs.
            let mut inner = self.inner.lock().unwrap();
            inner.update_commit_index(true);
            inner.apply();
        }
    }

    /// ## Candidate functions

    async fn run_candidate(&self) {
        let vote_request = self.vote_request().fuse();
        let timer = self.candidate_timer().fuse();
        pin_mut!(vote_request, timer);
        select_biased! {
            _ = vote_request => return,
            _ = timer => return,
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

        if self.role() != Role::Candidate { return; }

        // collect votes
        let majority = (self.peers.len() + 1) >> 1;
        let mut count = 1;
        loop {
            select_biased! {
                reply = rpcs.select_next_some() => {
                    match reply {
                        Ok(reply) => {
                            if reply.vote_granted { count += 1; }
                            let mut inner = self.inner.lock().unwrap();
                            if inner.handle_request_vote(reply) { return; }
                            if count >= majority && inner.role == Role::Candidate {
                                debug!("[{:?}] Wins election, turns to leader.", inner);
                                inner.role = Role::Leader;
                                return;
                            }
                        },
                        Err(_) => continue,
                    }
                },
                complete => break,
            }
        }
        // lose election
        let mut inner = self.inner.lock().unwrap();
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
            _ = timer => return,
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
        self.role() == Role::Leader
    }

    pub fn leader(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .leader
            .unwrap_or((self.me + 1) % self.peers.len())
    }

    pub fn log_size(&self) -> usize {
        self.inner.lock().unwrap().log_size
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and include) that index. Raft should now trim its log as much as possible.
    pub async fn snapshot(&self, index: usize, snapshot: &[u8]) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            assert!(index < inner.state.commit_index);
            inner.snapshot = snapshot.into();
            inner.last_included_log = Some(inner.state.logs[index].to_owned());
            inner.state.logs.trim_to(index + 1);
        }
        self.persist().await?;
        Ok(())
    }

    /// Save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    ///
    /// See paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self) -> io::Result<()> {
        let (persist, snapshot) = {
            let mut inner = self.inner.lock().unwrap();
            let persist = Persist {
                term: inner.state.term,
                voted_for: inner.state.voted_for,
                logs: inner.state.logs.clone(),
            };
            let snapshot = Snapshot {
                snapshot: inner.snapshot.clone(),
                last_included_log: inner.last_included_log.clone(),
            };
            let persist = bincode::serialize(&persist).unwrap();
            let snapshot = bincode::serialize(&snapshot).unwrap();
            inner.log_size = persist.len();
            (persist, snapshot)
        };

        let f1 = async {
            let file = fs::File::create("state").await?;
            file.write_all_at(&persist, 0).await?;
            file.sync_all().await?;
            io::Result::<()>::Ok(())
        };

        let f2 = async {
            let file = fs::File::create("snapshot").await?;
            file.write_all_at(&snapshot, 0).await?;
            file.sync_all().await?;
            io::Result::<()>::Ok(())
        };
        
        let (r1, r2) = join!(f1, f2);
        r1.and(r2)
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        let f1 = async {
            match fs::read("state").await {
                Ok(persist) => {
                    let persist: Persist = bincode::deserialize(&persist).unwrap();
                    let mut inner = self.inner.lock().unwrap();
                    inner.state.term = persist.term;
                    inner.state.voted_for = persist.voted_for;
                    inner.state.logs = persist.logs;
                    Ok(())
                },
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e),
            }
        };

        let f2 = async {
            match fs::read("snapshot").await {
                Ok(snapshot) => {
                    let snapshot: Snapshot = bincode::deserialize(&snapshot).unwrap();
                    let mut inner = self.inner.lock().unwrap();
                    inner.snapshot = snapshot.snapshot;
                    inner.last_included_log = snapshot.last_included_log;
                    Ok(())
                },
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e),
            }
        };
        
        let (r1, r2) = join!(f1, f2);
        r1.and(r2)
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
        let this = self.clone();
        task::spawn(async move { this.persist().await }).detach();
        Ok(reply)
    }

    async fn on_append_entry(&self, args: AppendEntryArgs) -> Result<AppendEntryReply> {
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
        inner.last_included_log.is_some() && last_included_index < inner.state.commit_index && inner.snapshot_done
    }

    fn election_timeout() -> Duration {
        Duration::from_millis(rand::rng().gen_range(150..300))
    }

    fn heartbeat_timeout() -> Duration {
        Duration::from_millis(50)
    }
}

impl fmt::Debug for RaftHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>", self.me)
    }
}
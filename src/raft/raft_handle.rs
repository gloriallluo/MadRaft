use std::{
    io,
    sync::{Arc, Mutex, Weak},
    net::SocketAddr,
};
use crate::raft::{raft::*, args::*, log::*};
use futures::channel::mpsc;
use madsim::{time::Instant, fs, net};
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


/// # RaftHandle

#[derive(Clone)]
pub struct RaftHandle {
    me: usize,
    inner: Arc<Mutex<Raft>>,
}

impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let size = peers.len();
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            apply_ch,
            task: Vec::new(),
            role: Role::Follower,
            state: State::new(size),
            myself: Weak::new(),
            timer: Instant::now(),
            snapshot: Vec::new(),
        }));
        inner.lock().unwrap().myself = Arc::downgrade(&inner.clone());
        let handle = RaftHandle { me, inner };
        // initialize from state persisted before a crash
        handle.restore().await.expect("Failed to restore");
        handle.start_rpc_server();
        handle.inner.lock().unwrap().run_follower();

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

    pub async fn run(&mut self) {}

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.inner.lock().unwrap();
        raft.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.inner.lock().unwrap();
        raft.role == Role::Leader
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
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.on_request_vote(args)
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        self.persist().await.expect("Failed to persist");
        Ok(reply)
    }

    async fn on_append_entry(&self, args: AppendEntryArgs) -> Result<AppendEntryReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.on_append_entry(args)
        };
        Ok(reply)
    }

    async fn on_install_snapshot(&self, args: InstallSnapshotArgs) -> Result<InstallSnapshotReply> {
        let reply = {
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
}
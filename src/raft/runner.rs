use std::sync::{Mutex, Weak};
use crate::raft::raft::Raft;

pub trait RaftRunner {
    fn new(rf: Weak<Mutex<Raft>>) -> Self;
    fn run(&self);
}
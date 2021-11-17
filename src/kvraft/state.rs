use std::collections::HashMap;
use madsim::net;
use serde::{Deserialize, Serialize};
use crate::kvraft::msg::*;


pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message + Clone;
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}


#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Kv {
    data: HashMap<String, String>,
}

impl State for Kv {
    type Command = Op;
    type Output = String;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Get { key } => self.get(key),
            Op::Put { key, value } => self.put(key, value),
            Op::Append { key, value } => self.append(key, value),
        }
    }
}

impl Kv {
    fn get(&self, key: String) -> String {
        self.data
            .get(&key)
            .map_or("", |v| v.as_str())
            .to_string()
    }

    fn put(&mut self, key: String, value: String) -> String {
        self.data
            .insert(key, value)
            .unwrap_or("".to_string())
    }

    fn append(&mut self, key: String, value: String) -> String {
        self.data
            .get_mut(&key)
            .map_or("", |v| {
                v.push_str(&value);
                v.as_str()
            })
            .to_string()
    }
}
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Formatter};
use std::ops::{Index, IndexMut, Range, RangeFrom};

/// # LogEntry

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct LogEntry {
    pub(crate) term: u64,
    pub(crate) index: u64,
    pub(crate) data: Vec<u8>,
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(term: {}, index: {})", self.term, self.index)
    }
}

/// # Logs

#[derive(Clone, Serialize, Deserialize)]
pub struct Logs {
    offset: u64,
    logs: Vec<LogEntry>,
}

impl Logs {
    pub fn push(&mut self, log: LogEntry) {
        self.logs.push(log);
    }

    pub fn len(&self) -> u64 {
        self.logs.len() as u64
    }

    pub fn begin(&self) -> u64 {
        self.offset
    }

    pub fn end(&self) -> u64 {
        self.offset + self.logs.len() as u64
    }

    pub fn contains_index(&self, index: u64) -> bool {
        (self.offset..self.offset + self.len()).contains(&index)
    }

    pub fn last(&self) -> Option<&LogEntry> {
        self.logs.last()
    }

    pub fn append(&mut self, other: &mut Vec<LogEntry>) {
        self.logs.append(other);
    }

    /// trim logs from `index`.
    pub fn trim_from(&mut self, index: u64) {
        if self.contains_index(index) {
            let index = index - self.offset;
            self.logs.drain(index as usize..);
        }
    }

    /// trim logs to `index`.
    /// if `index` is not contained in logs, then discard all logs.
    pub fn trim_to(&mut self, index: u64) {
        if index == 0 {
            return;
        }
        if self.contains_index(index - 1) {
            let new_offset = index;
            let index = index - self.offset;
            self.logs.drain(..index as usize);
            self.offset = new_offset;
        } else {
            self.logs.clear();
            self.offset = index;
        }
    }

    #[allow(unused)]
    pub fn print_all(&self) {
        debug!("{:?}", self);
        debug!("{:?}", self.logs);
    }
}

impl Default for Logs {
    fn default() -> Self {
        // Add an empty log entry at start, to be consistent with tester.
        Self {
            offset: 0,
            logs: vec![LogEntry::default()],
        }
    }
}

impl Index<u64> for Logs {
    type Output = LogEntry;
    fn index(&self, index: u64) -> &Self::Output {
        &self.logs[(index - self.offset) as usize]
    }
}

impl IndexMut<u64> for Logs {
    fn index_mut(&mut self, index: u64) -> &mut Self::Output {
        &mut self.logs[(index - self.offset) as usize]
    }
}

impl Index<Range<u64>> for Logs {
    type Output = [LogEntry];
    fn index(&self, index: Range<u64>) -> &Self::Output {
        let range = Range {
            start: index.start - self.offset,
            end: index.end - self.offset,
        };
        &self.logs[range.start as usize..range.end as usize]
    }
}

impl Index<RangeFrom<u64>> for Logs {
    type Output = [LogEntry];
    fn index(&self, index: RangeFrom<u64>) -> &Self::Output {
        let range = RangeFrom {
            start: index.start - self.offset,
        };
        &self.logs[range.start as usize..]
    }
}

impl Debug for Logs {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Logs({}..{})", self.offset, self.offset + self.len())
    }
}

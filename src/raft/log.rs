use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Formatter};
use std::ops::{Index, IndexMut, Range, RangeFrom};

/// # LogEntry

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct LogEntry {
    pub(crate) term: u64,
    pub(crate) index: usize,
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
    offset: usize,
    logs: Vec<LogEntry>,
}

impl Logs {
    pub fn push(&mut self, log: LogEntry) {
        self.logs.push(log);
    }

    pub fn len(&self) -> usize {
        self.logs.len()
    }

    pub fn begin(&self) -> usize {
        self.offset
    }

    pub fn end(&self) -> usize {
        self.offset + self.logs.len()
    }

    pub fn contains_index(&self, index: usize) -> bool {
        (self.offset..self.offset + self.len()).contains(&index)
    }

    pub fn last(&self) -> Option<&LogEntry> {
        self.logs.last()
    }

    pub fn append(&mut self, other: &mut Vec<LogEntry>) {
        self.logs.append(other);
    }

    /// trim logs from `index`.
    pub fn trim_from(&mut self, index: usize) {
        if self.contains_index(index) {
            let index = index - self.offset;
            self.logs.drain(index..);
        }
    }

    /// trim logs to `index`.
    /// if `index` is not contained in logs, then discard all logs.
    pub fn trim_to(&mut self, index: usize) {
        if index == 0 {
            return;
        }
        if self.contains_index(index - 1) {
            let new_offset = index;
            let index = index - self.offset;
            self.logs.drain(..index);
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

impl Index<usize> for Logs {
    type Output = LogEntry;
    fn index(&self, index: usize) -> &Self::Output {
        &self.logs[index - self.offset]
    }
}

impl IndexMut<usize> for Logs {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.logs[index - self.offset]
    }
}

impl Index<Range<usize>> for Logs {
    type Output = [LogEntry];
    fn index(&self, index: Range<usize>) -> &Self::Output {
        let range = Range {
            start: index.start - self.offset,
            end: index.end - self.offset,
        };
        &self.logs[range]
    }
}

impl Index<RangeFrom<usize>> for Logs {
    type Output = [LogEntry];
    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        let range = RangeFrom {
            start: index.start - self.offset,
        };
        &self.logs[range]
    }
}

// impl From<Vec<LogEntry>> for Logs {
//     fn from(logs: Vec<LogEntry>) -> Self {
//         if logs.is_empty() {
//             Self { offset: 0, logs }
//         } else {
//             Self { offset: logs[0].index, logs }
//         }
//     }
// }

// impl Into<Vec<LogEntry>> for Logs {
//     fn into(self) -> Vec<LogEntry> {
//         self.logs
//     }
// }

impl Debug for Logs {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Logs({}..{})", self.offset, self.offset + self.len())
    }
}

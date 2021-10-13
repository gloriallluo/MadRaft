use std::ops::{Range, Index, IndexMut, RangeFrom};
use std::fmt::{self, Debug, Formatter};
use serde::{Deserialize, Serialize};

/// # LogEntry

#[derive(Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub(crate) term: u64,
    pub(crate) index: usize,
    pub(crate) data: Vec<u8>,
}

impl Default for LogEntry {
    fn default() -> Self {
        Self { term: 0, index: 0, data: vec![] }
    }
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{ term: {}, index: {} }}", self.term, self.index)
    }
}


/// # Logs
/// Its' index may be different from its' real ones.

#[derive(Clone)]
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

    pub fn head(&self) -> usize {
        self.offset
    }

    pub fn tail(&self) -> usize {
        self.offset + self.logs.len()
    }

    pub fn contains_index(&self, index: usize) -> bool {
        (self.offset..self.offset + self.len()).contains(&index)
    }

    pub fn first(&self) -> Option<&LogEntry> {
        self.logs.first()
    }

    pub fn first_index(&self) -> Option<usize> {
        Some(self.logs.first()?.index)
    }

    pub fn last(&self) -> Option<&LogEntry> {
        self.logs.last()
    }

    pub fn last_index(&self) -> Option<usize> {
        Some(self.logs.last()?.index)
    }

    pub fn trim(&mut self, index: usize) {
        let new_offset = index + 1;
        let index = index - self.offset;
        self.logs.drain(..=index);
        self.offset = new_offset;
    }
}

impl Default for Logs {
    fn default() -> Self {
        // Add an empty log entry at start, to be consistent with tester.
        Self { offset: 0, logs: vec![LogEntry::default()], }
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

impl From<Vec<LogEntry>> for Logs {
    fn from(logs: Vec<LogEntry>) -> Self {
        if logs.is_empty() {
            Self { offset: 0, logs }
        } else {
            Self { offset: logs[0].index, logs }
        }
    }
}

impl Into<Vec<LogEntry>> for Logs {
    fn into(self) -> Vec<LogEntry> {
        self.logs
    }
}

impl Debug for Logs {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Logs({}..{})", self.offset, self.offset + self.len())
    }
}

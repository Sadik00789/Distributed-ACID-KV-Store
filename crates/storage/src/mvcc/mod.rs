pub mod key;
pub mod reader;

pub use key::KeyEncoder;
pub use reader::{MvccError, MvccReader};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    Put = 0,
    Delete = 1,
    Rollback = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRecord {
    pub start_ts: u64,
    pub op: OpType,
}

impl WriteRecord {
    pub fn new(start_ts: u64, op: OpType) -> Self {
        Self { start_ts, op }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(9);
        buf.push(self.op as u8);
        buf.extend_from_slice(&self.start_ts.to_be_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 9 {
            return None;
        }
        let op = match bytes[0] {
            0 => OpType::Put,
            1 => OpType::Delete,
            2 => OpType::Rollback,
            _ => return None,
        };
        let start_ts = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        Some(Self { start_ts, op })
    }
}

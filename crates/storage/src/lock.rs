use crate::mvcc::OpType;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LockError {
    #[error("Failed to decode lock bytes: {0}")]
    DecodeError(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lock {
    pub primary_key: Vec<u8>,
    pub start_ts: u64,
    pub ttl: u64,
    pub op: OpType,
}

impl Lock {
    pub fn new(primary_key: Vec<u8>, start_ts: u64, ttl: u64, op: OpType) -> Self {
        Self {
            primary_key,
            start_ts,
            ttl,
            op,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 8 + 8 + 4 + self.primary_key.len());
        buf.push(self.op as u8);
        buf.extend_from_slice(&self.start_ts.to_be_bytes());
        buf.extend_from_slice(&self.ttl.to_be_bytes());
        buf.extend_from_slice(&(self.primary_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.primary_key);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, LockError> {
        if bytes.len() < 21 {
            return Err(LockError::DecodeError("Payload too short".to_string()));
        }

        let op = match bytes[0] {
            0 => OpType::Put,
            1 => OpType::Delete,
            _ => return Err(LockError::DecodeError("Invalid OpType byte".to_string())),
        };

        let start_ts = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        let ttl = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
        let primary_len = u32::from_be_bytes(bytes[17..21].try_into().unwrap()) as usize;

        if bytes.len() < 21 + primary_len {
            return Err(LockError::DecodeError(
                "Truncated primary key bytes".to_string(),
            ));
        }

        let primary_key = bytes[21..21 + primary_len].to_vec();

        Ok(Self {
            primary_key,
            start_ts,
            ttl,
            op,
        })
    }
}

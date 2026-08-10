use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum KeyError {
    #[error("Invalid key format: byte stream too short to extract timestamp")]
    TooShort,
}

pub struct KeyEncoder;

impl KeyEncoder {
    /// Encodes a user key and timestamp into a lexicographically ordered byte vector.
    /// Timestamp is bit-wise inverted so higher timestamps sort BEFORE lower timestamps.
    pub fn encode(user_key: &[u8], ts: u64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(user_key.len() + 8);
        encoded.extend_from_slice(user_key);
        encoded.extend_from_slice(&(!ts).to_be_bytes());
        encoded
    }

    /// Decodes an encoded byte slice back into the user key and physical timestamp.
    pub fn decode(encoded: &[u8]) -> Result<(&[u8], u64), KeyError> {
        if encoded.len() < 8 {
            return Err(KeyError::TooShort);
        }
        let split_idx = encoded.len() - 8;
        let user_key = &encoded[..split_idx];
        let mut ts_bytes = [0u8; 8];
        ts_bytes.copy_from_slice(&encoded[split_idx..]);
        let inverted_ts = u64::from_be_bytes(ts_bytes);
        Ok((user_key, !inverted_ts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_encoding_decoding() {
        let user_key = b"account:1001";
        let ts = 1718000000u64;

        let encoded = KeyEncoder::encode(user_key, ts);
        let (decoded_key, decoded_ts) = KeyEncoder::decode(&encoded).unwrap();

        assert_eq!(decoded_key, user_key);
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn test_timestamp_sorting_order() {
        let user_key = b"account:1001";
        let k_older = KeyEncoder::encode(user_key, 100);
        let k_newer = KeyEncoder::encode(user_key, 200);

        // Newer timestamp must sort BEFORE older timestamp in RocksDB
        assert!(k_newer < k_older);
    }
}

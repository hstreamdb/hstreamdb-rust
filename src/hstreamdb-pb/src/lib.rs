#![allow(clippy::derive_partial_eq_without_eq)]
tonic::include_proto!("hstream.server");

use std::fmt;

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.shard_id, self.batch_id, self.batch_index
        )
    }
}

pub mod kv {
    tonic::include_proto!("kv");
}

pub mod raft {
    tonic::include_proto!("raft");
}

pub mod txn {
    tonic::include_proto!("txn");
}

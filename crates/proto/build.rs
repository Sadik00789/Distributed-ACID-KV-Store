fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/raft.proto");
    println!("cargo:rerun-if-changed=../../proto/txn.proto");
    println!("cargo:rerun-if-changed=../../proto/kv.proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../proto/raft.proto",
                "../../proto/txn.proto",
                "../../proto/kv.proto",
            ],
            &["../../proto"],
        )?;

    Ok(())
}

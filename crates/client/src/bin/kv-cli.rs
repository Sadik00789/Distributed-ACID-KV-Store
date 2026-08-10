use clap::{Parser, Subcommand};
use client::KvClient;

#[derive(Parser, Debug)]
#[command(author, version, about = "Distributed ACID KV Store CLI")]
struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Read a value by key
    Get { key: String },
    /// Write a key-value pair via 2PC transaction
    Put { key: String, value: String },
    /// Scan key-value pairs in range [start_key, end_key)
    Scan {
        start_key: String,
        end_key: String,
        #[arg(default_value_t = 100)]
        limit: u32,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut client = KvClient::connect(cli.endpoint).await?;

    match cli.command {
        Commands::Get { key } => match client.get(key.into_bytes()).await? {
            Some(val) => println!("{}", String::from_utf8_lossy(&val)),
            None => println!("(nil)"),
        },
        Commands::Put { key, value } => {
            let key_bytes = key.into_bytes();
            let val_bytes = value.into_bytes();

            // Generate physical time start timestamp for 2PC
            let physical_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64;
            let start_ts = physical_now << 16;

            let mutations = vec![(key_bytes.clone(), Some(val_bytes))];

            // 1. Prewrite phase
            client
                .txn_prewrite(start_ts, mutations, key_bytes.clone(), 3000)
                .await?;

            // 2. Commit phase
            let commit_ts = start_ts + 1;
            client
                .txn_commit(start_ts, commit_ts, vec![key_bytes])
                .await?;

            println!("OK");
        }
        Commands::Scan {
            start_key,
            end_key,
            limit,
        } => {
            let results = client
                .scan(start_key.into_bytes(), end_key.into_bytes(), limit)
                .await?;
            for (k, v) in results {
                println!(
                    "{}: {}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
            }
        }
    }

    Ok(())
}

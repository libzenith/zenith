use s3_scrubber::garbage::{find_garbage, purge_garbage, PurgeMode};
use s3_scrubber::scan_metadata::scan_metadata;
use s3_scrubber::{init_logging, BucketConfig, ConsoleConfig, NodeKind, TraversingDepth};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short, long, default_value_t = false)]
    delete: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    FindGarbage {
        #[arg(short, long)]
        node_kind: NodeKind,
        #[arg(short, long, default_value_t=TraversingDepth::Tenant)]
        depth: TraversingDepth,
        #[arg(short, long, default_value_t = String::from("garbage.json"))]
        output_path: String,
    },
    PurgeGarbage {
        #[arg(short, long)]
        input_path: String,
        #[arg(short, long, default_value_t = PurgeMode::DeletedOnly)]
        mode: PurgeMode,
    },
    ScanMetadata {
        #[arg(short, long, default_value_t = false)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let bucket_config = BucketConfig::from_env()?;

    let command_log_name = match &cli.command {
        Command::ScanMetadata { .. } => "scan",
        Command::FindGarbage { .. } => "find-garbage",
        Command::PurgeGarbage { .. } => "purge-garbage",
    };
    let _guard = init_logging(&format!(
        "{}_{}_{}_{}.log",
        std::env::args().next().unwrap(),
        command_log_name,
        bucket_config.bucket,
        chrono::Utc::now().format("%Y_%m_%d__%H_%M_%S")
    ));

    match cli.command {
        Command::ScanMetadata { json } => match scan_metadata(bucket_config).await {
            Err(e) => {
                tracing::error!("Failed: {e}");
                Err(e)
            }
            Ok(summary) => {
                if json {
                    println!("{}", serde_json::to_string(&summary).unwrap())
                } else {
                    println!("{}", summary.summary_string());
                }
                if summary.is_fatal() {
                    Err(anyhow::anyhow!("Fatal scrub errors detected"))
                } else {
                    Ok(())
                }
            }
        },
        Command::FindGarbage {
            node_kind,
            depth,
            output_path,
        } => {
            let console_config = ConsoleConfig::from_env()?;
            find_garbage(bucket_config, console_config, depth, node_kind, output_path).await
        }
        Command::PurgeGarbage { input_path, mode } => {
            purge_garbage(input_path, mode, !cli.delete).await
        }
    }
}

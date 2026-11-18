mod checkpoint;
mod checkpoint_lock;
mod cli;
mod config;
mod event;
mod metrics;
mod pipeline;
mod sink;
mod source;

use anyhow::Result;
use clap::Parser;
use cli::{CheckpointCommand, Cli, Command, SourceCommand};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .with_target(false)
        .json()
        .init();

    match Cli::parse().command {
        Command::Init { output } => config::write_example(&output)?,
        Command::Validate { config: path } => {
            let cfg = config::Config::load(&path)?;
            cfg.validate()?;
            println!("configuration is valid");
        }
        Command::Run {
            config: path,
            sink: _,
        } => {
            let cfg = config::Config::load(&path)?;
            cfg.validate()?;
            pipeline::run(cfg).await?;
        }
        Command::Source {
            command: SourceCommand::Check { config: path },
        } => {
            let cfg = config::Config::load(&path)?;
            cfg.validate()?;
            source::check(&cfg).await?;
            println!("MySQL connection is available and row-based binary logging is enabled");
        }
        Command::Checkpoint {
            command: CheckpointCommand::Show { config: path },
        } => {
            let cfg = config::Config::load(&path)?;
            match checkpoint::load(std::path::Path::new(&cfg.checkpoint.path))? {
                Some(value) => println!("{}", serde_json::to_string_pretty(&value)?),
                None => println!("no checkpoint"),
            }
        }
        Command::Checkpoint {
            command: CheckpointCommand::Reset { config: path, yes },
        } => {
            let cfg = config::Config::load(&path)?;
            if !yes {
                anyhow::bail!("reset removes the durable restart position; pass --yes to confirm");
            }
            checkpoint::reset(std::path::Path::new(&cfg.checkpoint.path))?;
            println!("checkpoint removed");
        }
        Command::Status { config: path } => {
            let cfg = config::Config::load(&path)?;
            match checkpoint::load(std::path::Path::new(&cfg.checkpoint.path))? {
                Some(value) => println!(
                    "last committed source position: {}:{}",
                    value.binlog_file, value.binlog_position
                ),
                None => println!("no durable checkpoint; source will start at its current end"),
            }
        }
    }
    Ok(())
}

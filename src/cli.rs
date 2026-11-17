use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "binflux",
    version,
    about = "MySQL change data capture to JSON Lines"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Init {
        #[arg(short, long, default_value = "binflux.yaml")]
        output: PathBuf,
    },
    Validate {
        #[arg(short, long, default_value = "binflux.yaml")]
        config: PathBuf,
    },
    Run {
        #[arg(short, long, default_value = "binflux.yaml")]
        config: PathBuf,
        #[arg(long, value_enum, default_value_t = SinkKind::Stdout)]
        sink: SinkKind,
    },
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
    },
    Status {
        #[arg(short, long, default_value = "binflux.yaml")]
        config: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub enum SourceCommand {
    Check {
        #[arg(short, long, default_value = "binflux.yaml")]
        config: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub enum CheckpointCommand {
    Show {
        #[arg(short, long, default_value = "binflux.yaml")]
        config: PathBuf,
    },
    Reset {
        #[arg(short, long, default_value = "binflux.yaml")]
        config: PathBuf,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SinkKind {
    Stdout,
}

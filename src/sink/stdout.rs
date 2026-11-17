use super::Sink;
use crate::event::CdcEvent;
use anyhow::{Context, Result};
use std::io::{BufWriter, Write};

pub struct StdoutSink<W: Write> {
    writer: BufWriter<W>,
}

impl<W: Write> StdoutSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
        }
    }
}

impl<W: Write> Sink for StdoutSink<W> {
    fn write_transaction(&mut self, events: &[CdcEvent]) -> Result<()> {
        for event in events {
            serde_json::to_writer(&mut self.writer, event)?;
            self.writer.write_all(b"\n")?;
        }
        self.writer
            .flush()
            .context("flushing committed JSONL transaction")
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("flushing JSONL stdout")
    }
}

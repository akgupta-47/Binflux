use super::Sink;
use crate::event::CdcEvent;
use anyhow::{Context, Result};
use std::{
    io::{BufWriter, Write},
    sync::Mutex,
};

pub struct StdoutSink<W: Write + Send> {
    writer: Mutex<BufWriter<W>>,
}

impl<W: Write + Send> StdoutSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(BufWriter::new(writer)),
        }
    }
}

#[async_trait::async_trait]
impl<W: Write + Send> Sink for StdoutSink<W> {
    async fn write_transaction(&self, _sequence: usize, events: &[CdcEvent]) -> Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        for event in events {
            serde_json::to_writer(&mut *writer, event)?;
            writer.write_all(b"\n")?;
        }
        writer
            .flush()
            .context("flushing committed JSONL transaction")
    }

    async fn flush(&self) -> Result<()> {
        self.writer
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .flush()
            .context("flushing JSONL stdout")
    }
}

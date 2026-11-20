pub mod kafka;
pub mod stdout;

use crate::event::CdcEvent;
use anyhow::Result;

#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn write_transaction(&self, sequence: usize, events: &[CdcEvent]) -> Result<()>;
    async fn flush(&self) -> Result<()>;
    async fn close(&self) -> Result<()> {
        self.flush().await
    }
}

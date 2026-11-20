pub mod kafka;
pub mod stdout;

use crate::event::CdcEvent;
use anyhow::Result;

#[async_trait::async_trait(?Send)]
pub trait Sink {
    async fn write_transaction(&mut self, events: &[CdcEvent]) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()> {
        self.flush().await
    }
}

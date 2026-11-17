pub mod stdout;

use crate::event::CdcEvent;
use anyhow::Result;

pub trait Sink {
    fn write_transaction(&mut self, events: &[CdcEvent]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn close(&mut self) -> Result<()> {
        self.flush()
    }
}

pub mod raw;
pub mod tui;

use std::time::Duration;

/// Event after correlation — ready for display.
#[derive(Clone, Debug)]
pub struct DisplayEvent {
    pub wall_time: chrono::DateTime<chrono::Local>,
    pub conn_id: u64,
    pub kind: DisplayEventKind,
}

#[derive(Clone, Debug)]
pub enum DisplayEventKind {
    Query {
        sql: String,
        duration: Duration,
        rows: Option<u64>,
    },
    Error {
        #[allow(dead_code)]
        sql: Option<String>,
        duration: Option<Duration>,
        code: String,
        message: String,
    },
    ConnectionOpened,
    ConnectionClosed,
    Warning(String),
}

/// Processes display events.
pub trait OutputSink: Send + 'static {
    fn handle_event(&mut self, event: &DisplayEvent);
    fn shutdown(&mut self);
}

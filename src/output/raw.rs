use super::{DisplayEvent, DisplayEventKind, OutputSink};

/// Simple stdout line-by-line output, pipe-friendly.
pub struct RawSink;

impl RawSink {
    pub fn new() -> Self {
        Self
    }
}

impl OutputSink for RawSink {
    fn handle_event(&mut self, event: &DisplayEvent) {
        let time = event.wall_time.format("%H:%M:%S%.3f");
        let conn = event.conn_id;

        match &event.kind {
            DisplayEventKind::Query { sql, duration, rows } => {
                let ms = duration.as_secs_f64() * 1000.0;
                let rows_str = rows.map(|r| format!(" [{r} rows]")).unwrap_or_default();
                println!("{time} [conn:{conn}] {ms:>8.1}ms  {sql}{rows_str}");
            }
            DisplayEventKind::Error { code, message, duration, .. } => {
                let dur_str = duration
                    .map(|d| format!("{:>8.1}ms", d.as_secs_f64() * 1000.0))
                    .unwrap_or_else(|| "        ".to_string());
                println!("{time} [conn:{conn}] {dur_str}  ERR {code}: {message}");
            }
            DisplayEventKind::ConnectionOpened => {
                println!("{time} [conn:{conn}]            ++ connection opened");
            }
            DisplayEventKind::ConnectionClosed => {
                println!("{time} [conn:{conn}]            -- connection closed");
            }
            DisplayEventKind::Warning(msg) => {
                println!("{time} [conn:{conn}]            WARN: {msg}");
            }
        }
    }

    fn shutdown(&mut self) {
        // No-op
    }
}

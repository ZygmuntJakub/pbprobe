use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::fingerprint::fingerprint;
use crate::output::{DisplayEvent, DisplayEventKind};
use crate::protocol::{ProtoEvent, TxStatus};

pub struct StatsCollector {
    connections: HashMap<u64, ConnState>,
    pub fingerprints: HashMap<String, QueryAggregates>,
    pub latency_buckets: [u64; 6], // <1ms, 1-5, 5-10, 10-50, 50-100, 100+
    pub total_queries: u64,
    pub total_errors: u64,
    pub active_connections: u64,
    qps_window: VecDeque<Instant>,
    pub first_query_at: Option<Instant>,
    pub last_query_at: Option<Instant>,
}

struct ConnState {
    pending_queries: VecDeque<PendingQuery>,
    in_transaction: bool,
}

struct PendingQuery {
    sql: String,
    started_at: Instant,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryAggregates {
    pub fingerprint: String,
    pub count: u64,
    pub total_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            fingerprints: HashMap::new(),
            latency_buckets: [0; 6],
            total_queries: 0,
            total_errors: 0,
            active_connections: 0,
            qps_window: VecDeque::new(),
            first_query_at: None,
            last_query_at: None,
        }
    }

    /// Reset all accumulated stats for a fresh measurement window.
    /// Keeps connections and active_connections intact (live state).
    pub fn reset(&mut self) {
        self.fingerprints.clear();
        self.latency_buckets = [0; 6];
        self.total_queries = 0;
        self.total_errors = 0;
        self.qps_window.clear();
        self.first_query_at = None;
        self.last_query_at = None;
    }

    pub fn process_event(&mut self, conn_id: u64, event: ProtoEvent) -> Option<DisplayEvent> {
        let now = Instant::now();
        let wall_time = chrono::Local::now();

        match event {
            ProtoEvent::QueryStart { sql } => {
                let conn = self.ensure_conn(conn_id);
                conn.pending_queries.push_back(PendingQuery {
                    sql,
                    started_at: now,
                });
                None
            }

            ProtoEvent::ParseDetected { sql } => {
                // Parse != Execute — don't push to queue. Keep the warning for visibility.
                Some(DisplayEvent {
                    wall_time,
                    conn_id,
                    kind: DisplayEventKind::Warning(format!(
                        "Extended query protocol: {}",
                        truncate(&sql, 80)
                    )),
                })
            }

            ProtoEvent::QueryComplete { rows, .. } => {
                let conn = self.connections.get_mut(&conn_id)?;
                let pending = conn.pending_queries.pop_front()?;
                let duration = now - pending.started_at;

                self.total_queries += 1;
                if self.first_query_at.is_none() {
                    self.first_query_at = Some(now);
                }
                self.last_query_at = Some(now);
                self.record_latency(duration);
                self.record_fingerprint(&pending.sql, duration);
                self.qps_window.push_back(now);

                Some(DisplayEvent {
                    wall_time,
                    conn_id,
                    kind: DisplayEventKind::Query {
                        sql: pending.sql,
                        duration,
                        rows,
                    },
                })
            }

            ProtoEvent::QueryError { severity, code, message } => {
                self.total_errors += 1;

                // Pop the failed query from the front of the queue
                let (sql, duration) = self.connections.get_mut(&conn_id)
                    .and_then(|c| c.pending_queries.pop_front())
                    .map(|p| (Some(p.sql), Some(now - p.started_at)))
                    .unwrap_or((None, None));

                if severity == "ERROR" || severity == "FATAL" {
                    Some(DisplayEvent {
                        wall_time,
                        conn_id,
                        kind: DisplayEventKind::Error {
                            sql,
                            duration,
                            code,
                            message,
                        },
                    })
                } else {
                    None
                }
            }

            ProtoEvent::ConnectionReady { status } => {
                let conn = self.connections.get_mut(&conn_id)?;
                conn.in_transaction = status == TxStatus::InTransaction;
                // Clear any orphaned pending queries (error mid-pipeline skips remaining Executes)
                conn.pending_queries.clear();
                None
            }

            ProtoEvent::ConnectionClosed => {
                self.connections.remove(&conn_id);
                self.active_connections = self.active_connections.saturating_sub(1);
                Some(DisplayEvent {
                    wall_time,
                    conn_id,
                    kind: DisplayEventKind::ConnectionClosed,
                })
            }

            ProtoEvent::Unknown { .. } => None,
        }
    }

    pub fn connection_opened(&mut self, conn_id: u64) -> DisplayEvent {
        self.active_connections += 1;
        self.connections.insert(conn_id, ConnState {
            pending_queries: VecDeque::new(),
            in_transaction: false,
        });
        DisplayEvent {
            wall_time: chrono::Local::now(),
            conn_id,
            kind: DisplayEventKind::ConnectionOpened,
        }
    }

    pub fn connection_dropped(&mut self, conn_id: u64) -> Option<DisplayEvent> {
        if self.connections.remove(&conn_id).is_some() {
            self.active_connections = self.active_connections.saturating_sub(1);
            Some(DisplayEvent {
                wall_time: chrono::Local::now(),
                conn_id,
                kind: DisplayEventKind::ConnectionClosed,
            })
        } else {
            None
        }
    }

    fn ensure_conn(&mut self, conn_id: u64) -> &mut ConnState {
        self.connections.entry(conn_id).or_insert_with(|| ConnState {
            pending_queries: VecDeque::new(),
            in_transaction: false,
        })
    }

    fn record_latency(&mut self, duration: Duration) {
        let ms = duration.as_secs_f64() * 1000.0;
        let bucket = match ms {
            ms if ms < 1.0 => 0,
            ms if ms < 5.0 => 1,
            ms if ms < 10.0 => 2,
            ms if ms < 50.0 => 3,
            ms if ms < 100.0 => 4,
            _ => 5,
        };
        self.latency_buckets[bucket] += 1;
    }

    fn record_fingerprint(&mut self, sql: &str, duration: Duration) {
        let fp = fingerprint(sql);
        let agg = self.fingerprints.entry(fp.clone()).or_insert_with(|| QueryAggregates {
            fingerprint: fp,
            count: 0,
            total_duration: Duration::ZERO,
            min_duration: Duration::MAX,
            max_duration: Duration::ZERO,
        });
        agg.count += 1;
        agg.total_duration += duration;
        agg.min_duration = agg.min_duration.min(duration);
        agg.max_duration = agg.max_duration.max(duration);
    }

    /// Queries per second over a sliding 1-second window.
    pub fn qps(&mut self) -> u64 {
        let cutoff = Instant::now() - Duration::from_secs(1);
        // VecDeque is sorted by insertion time — pop expired entries from the front
        while self.qps_window.front().is_some_and(|&t| t <= cutoff) {
            self.qps_window.pop_front();
        }
        self.qps_window.len() as u64
    }

    pub fn top_queries(&self, n: usize) -> Vec<QueryAggregates> {
        let mut queries: Vec<_> = self.fingerprints.values().cloned().collect();
        queries.sort_unstable_by(|a, b| b.total_duration.cmp(&a.total_duration));
        queries.truncate(n);
        queries
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

use std::collections::{HashMap, VecDeque};
use std::io;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, BarChart};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::proxy::ProxyMessage;
use crate::stats::{FrozenStats, QueryAggregates, StatsCollector};
use super::{DisplayEvent, DisplayEventKind};

#[derive(Serialize, Deserialize)]
struct Snapshot {
    timestamp: String,
    total_queries: u64,
    total_errors: u64,
    active_connections: u64,
    latency_buckets: LatencyBuckets,
    top_queries: Vec<SnapshotQuery>,
    recent_events: Vec<SnapshotEvent>,
}

#[derive(Serialize, Deserialize)]
struct LatencyBuckets {
    under_1ms: u64,
    ms_1_5: u64,
    ms_5_10: u64,
    ms_10_50: u64,
    ms_50_100: u64,
    over_100ms: u64,
}

#[derive(Serialize, Deserialize)]
struct SnapshotQuery {
    fingerprint: String,
    count: u64,
    avg_ms: f64,
    min_ms: f64,
    max_ms: f64,
}

#[derive(Serialize, Deserialize)]
struct SnapshotEvent {
    time: String,
    conn_id: u64,
    latency: String,
    message: String,
}

const MAX_EVENTS: usize = 10_000;

#[derive(Clone)]
struct QueryRow {
    time: String,
    instant: Instant,
    conn_id: u64,
    latency: String,
    /// Raw SQL for query events (used for fingerprint toggle), None for non-query rows.
    raw_sql: Option<String>,
    rows_suffix: String,
    /// Pre-formatted display text for non-query events; ignored when raw_sql is Some.
    display: String,
    style: Style,
}

struct FrozenTab {
    label: String,
    events: VecDeque<QueryRow>,
    stats: FrozenStats,
    scroll_offset: usize,
    auto_scroll: bool,
    show_fingerprints: bool,
}

/// Shared context for draw methods — abstracts over live and frozen tabs.
struct DrawContext<'a> {
    events: &'a VecDeque<QueryRow>,
    fingerprints: &'a HashMap<String, QueryAggregates>,
    latency_buckets: &'a [u64; 6],
    total_queries: u64,
    total_errors: u64,
    active_connections: u64,
    first_query_at: Option<Instant>,
    scroll_offset: &'a mut usize,
    auto_scroll: bool,
    show_fingerprints: bool,
    is_frozen: bool,
    qps: Option<u64>,
}

enum InputMode {
    Normal,
    SavePrompt { buffer: String, cursor: usize },
    ImportPrompt { buffer: String, cursor: usize },
}

pub struct TuiApp {
    events: VecDeque<QueryRow>,
    stats: StatsCollector,
    scroll_offset: usize,
    auto_scroll: bool,
    paused: bool,
    show_fingerprints: bool,
    listen_port: u16,
    upstream: String,
    threshold_ms: u64,
    should_quit: bool,
    frozen_tabs: Vec<FrozenTab>,
    /// 0 = live tab, 1+ = frozen_tabs[active_tab - 1]
    active_tab: usize,
    next_tab_id: usize,
    input_mode: InputMode,
}

impl TuiApp {
    fn new(listen_port: u16, upstream: String, threshold_ms: u64) -> Self {
        Self {
            events: VecDeque::with_capacity(MAX_EVENTS),
            stats: StatsCollector::new(),
            scroll_offset: 0,
            auto_scroll: true,
            paused: false,
            show_fingerprints: false,
            listen_port,
            upstream,
            threshold_ms,
            should_quit: false,
            frozen_tabs: Vec::new(),
            active_tab: 0,
            next_tab_id: 1,
            input_mode: InputMode::Normal,
        }
    }

    fn push_event(&mut self, display_event: &DisplayEvent) {
        if self.paused {
            return;
        }

        let time = display_event.wall_time.format("%H:%M:%S%.3f").to_string();
        let conn_id = display_event.conn_id;

        let (latency, raw_sql, rows_suffix, display, style) = match &display_event.kind {
            DisplayEventKind::Query { sql, duration, rows } => {
                let ms = duration.as_secs_f64() * 1000.0;
                let latency = format!("{ms:.1}ms");
                let rows_suffix = rows.map(|r| format!(" [{r}]")).unwrap_or_default();
                let style = latency_style(ms, self.threshold_ms);
                (latency, Some(sql.clone()), rows_suffix, String::new(), style)
            }
            DisplayEventKind::Error { code, message, duration, .. } => {
                let dur = duration
                    .map(|d| format!("{:.1}ms", d.as_secs_f64() * 1000.0))
                    .unwrap_or_default();
                (
                    dur,
                    None,
                    String::new(),
                    format!("ERR {code}: {message}"),
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )
            }
            DisplayEventKind::ConnectionOpened => {
                ("".into(), None, String::new(), "++ connection opened".into(), Style::default().fg(Color::DarkGray))
            }
            DisplayEventKind::ConnectionClosed => {
                ("".into(), None, String::new(), "-- connection closed".into(), Style::default().fg(Color::DarkGray))
            }
            DisplayEventKind::Warning(msg) => {
                ("".into(), None, String::new(), format!("WARN: {msg}"), Style::default().fg(Color::Yellow))
            }
        };

        self.events.push_back(QueryRow {
            time,
            instant: Instant::now(),
            conn_id,
            latency,
            raw_sql,
            rows_suffix,
            display,
            style,
        });

        if self.events.len() > MAX_EVENTS {
            self.events.pop_front();
            if self.scroll_offset > 0 {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
            }
        }

        if self.auto_scroll {
            self.scroll_to_bottom();
        }
    }

    fn scroll_to_bottom(&mut self) {
        // Will be calculated during render based on visible area
        self.scroll_offset = usize::MAX;
    }

    // --- Tab lifecycle ---

    fn create_tab(&mut self) {
        let label = format!("Tab {}", self.next_tab_id);
        self.next_tab_id += 1;
        self.frozen_tabs.push(FrozenTab {
            label,
            events: self.events.clone(),
            stats: self.stats.freeze(),
            scroll_offset: self.scroll_offset,
            auto_scroll: self.auto_scroll,
            show_fingerprints: self.show_fingerprints,
        });
        // Stay on live tab — state kept; user can reset with 'r'
        self.active_tab = 0;
    }

    fn close_tab(&mut self) {
        if self.active_tab == 0 {
            return; // Can't close live tab
        }
        let idx = self.active_tab - 1;
        self.frozen_tabs.remove(idx);
        // If we were on the last frozen tab, move back
        if self.active_tab > self.frozen_tabs.len() {
            self.active_tab = self.frozen_tabs.len(); // last frozen, or 0 (live) if none left
        }
    }

    fn next_tab(&mut self) {
        let total = 1 + self.frozen_tabs.len(); // live + frozen
        self.active_tab = (self.active_tab + 1) % total;
    }

    fn prev_tab(&mut self) {
        let total = 1 + self.frozen_tabs.len();
        self.active_tab = (self.active_tab + total - 1) % total;
    }

    /// Returns mutable refs to (scroll_offset, auto_scroll, show_fingerprints)
    /// for the active tab — either live state or a frozen tab.
    fn active_scroll_state(&mut self) -> (&mut usize, &mut bool, &mut bool) {
        if self.active_tab == 0 {
            (&mut self.scroll_offset, &mut self.auto_scroll, &mut self.show_fingerprints)
        } else {
            let tab = &mut self.frozen_tabs[self.active_tab - 1];
            (&mut tab.scroll_offset, &mut tab.auto_scroll, &mut tab.show_fingerprints)
        }
    }

    fn handle_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        if !matches!(self.input_mode, InputMode::Normal) {
            self.handle_input_key(code);
            return;
        }

        match code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => self.should_quit = true,

            // Tab management
            KeyCode::Char('t') => self.create_tab(),
            KeyCode::Tab => self.next_tab(),
            KeyCode::BackTab => self.prev_tab(),
            KeyCode::Char('x') => self.close_tab(),
            KeyCode::Char(c @ '1'..='9') => {
                let n = (c as usize) - ('1' as usize);
                let total = 1 + self.frozen_tabs.len();
                if n < total {
                    self.active_tab = n;
                }
            }

            // Scroll keys — operate on active tab
            KeyCode::Char('j') | KeyCode::Down => {
                let (offset, auto_scroll, _) = self.active_scroll_state();
                *auto_scroll = false;
                *offset = offset.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                let (offset, auto_scroll, _) = self.active_scroll_state();
                *auto_scroll = false;
                *offset = offset.saturating_sub(1);
            }
            KeyCode::Char('G') | KeyCode::End => {
                let (offset, auto_scroll, _) = self.active_scroll_state();
                *auto_scroll = true;
                *offset = usize::MAX;
            }
            KeyCode::Char('g') | KeyCode::Home => {
                let (offset, auto_scroll, _) = self.active_scroll_state();
                *auto_scroll = false;
                *offset = 0;
            }
            KeyCode::PageDown => {
                let (offset, auto_scroll, _) = self.active_scroll_state();
                *auto_scroll = false;
                *offset = offset.saturating_add(20);
            }
            KeyCode::PageUp => {
                let (offset, auto_scroll, _) = self.active_scroll_state();
                *auto_scroll = false;
                *offset = offset.saturating_sub(20);
            }

            // Fingerprint toggle — operates on active tab
            KeyCode::Char('f') => {
                let (_, _, show_fp) = self.active_scroll_state();
                *show_fp = !*show_fp;
            }

            // Pause and reset — live tab only
            KeyCode::Char('p') => {
                if self.active_tab == 0 {
                    self.paused = !self.paused;
                }
            }
            KeyCode::Char('r') => {
                if self.active_tab == 0 {
                    self.stats.reset();
                    self.events.clear();
                    self.scroll_offset = 0;
                    self.auto_scroll = true;
                }
            }
            KeyCode::Char('s') => {
                let default = format!("dbprobe-{}.json", chrono::Local::now().format("%Y%m%dT%H%M%S"));
                let cursor = default.len();
                self.input_mode = InputMode::SavePrompt { buffer: default, cursor };
            }
            KeyCode::Char('i') => {
                self.input_mode = InputMode::ImportPrompt { buffer: String::new(), cursor: 0 };
            }
            _ => {}
        }
    }

    fn handle_input_key(&mut self, code: KeyCode) {
        let (buffer, cursor) = match &mut self.input_mode {
            InputMode::SavePrompt { buffer, cursor } |
            InputMode::ImportPrompt { buffer, cursor } => (buffer, cursor),
            InputMode::Normal => return,
        };

        match code {
            KeyCode::Char(c) => {
                buffer.insert(*cursor, c);
                *cursor += c.len_utf8();
            }
            KeyCode::Backspace => {
                if *cursor > 0 {
                    // Find previous char boundary
                    let mut new_cursor = *cursor - 1;
                    while new_cursor > 0 && !buffer.is_char_boundary(new_cursor) {
                        new_cursor -= 1;
                    }
                    buffer.drain(new_cursor..*cursor);
                    *cursor = new_cursor;
                }
            }
            KeyCode::Left => {
                if *cursor > 0 {
                    *cursor -= 1;
                    while *cursor > 0 && !buffer.is_char_boundary(*cursor) {
                        *cursor -= 1;
                    }
                }
            }
            KeyCode::Right => {
                if *cursor < buffer.len() {
                    *cursor += 1;
                    while *cursor < buffer.len() && !buffer.is_char_boundary(*cursor) {
                        *cursor += 1;
                    }
                }
            }
            KeyCode::Enter => {
                // Take ownership of buffer via swap
                let mode = std::mem::replace(&mut self.input_mode, InputMode::Normal);
                match mode {
                    InputMode::SavePrompt { buffer, .. } => {
                        if !buffer.is_empty() {
                            self.save_to_path(&buffer);
                        }
                    }
                    InputMode::ImportPrompt { buffer, .. } => {
                        if !buffer.is_empty() {
                            self.import_from_path(&buffer);
                        }
                    }
                    InputMode::Normal => {}
                }
            }
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
            }
            _ => {}
        }
    }

    fn save_to_path(&mut self, path: &str) {
        let now = chrono::Local::now();

        // Build snapshot from active tab's data
        let (buckets, total_queries, total_errors, active_connections, top_queries, events) =
            if self.active_tab == 0 {
                (
                    &self.stats.latency_buckets,
                    self.stats.total_queries,
                    self.stats.total_errors,
                    self.stats.active_connections,
                    self.stats.top_queries(20),
                    &self.events,
                )
            } else if let Some(tab) = self.frozen_tabs.get(self.active_tab - 1) {
                (
                    &tab.stats.latency_buckets,
                    tab.stats.total_queries,
                    tab.stats.total_errors,
                    tab.stats.active_connections,
                    tab.stats.top_queries(20),
                    &tab.events,
                )
            } else {
                return;
            };

        let snapshot = Snapshot {
            timestamp: now.to_rfc3339(),
            total_queries,
            total_errors,
            active_connections,
            latency_buckets: LatencyBuckets {
                under_1ms: buckets[0],
                ms_1_5: buckets[1],
                ms_5_10: buckets[2],
                ms_10_50: buckets[3],
                ms_50_100: buckets[4],
                over_100ms: buckets[5],
            },
            top_queries: top_queries.into_iter().map(|q| {
                let avg_ms = if q.count > 0 {
                    q.total_duration.as_secs_f64() * 1000.0 / q.count as f64
                } else {
                    0.0
                };
                SnapshotQuery {
                    fingerprint: q.fingerprint,
                    count: q.count,
                    avg_ms,
                    min_ms: q.min_duration.as_secs_f64() * 1000.0,
                    max_ms: q.max_duration.as_secs_f64() * 1000.0,
                }
            }).collect(),
            recent_events: events.iter().map(|row| {
                let message = match &row.raw_sql {
                    Some(sql) => format!("{sql}{}", row.rows_suffix),
                    None => row.display.clone(),
                };
                SnapshotEvent {
                    time: row.time.clone(),
                    conn_id: row.conn_id,
                    latency: row.latency.clone(),
                    message,
                }
            }).collect(),
        };

        let message = match serde_json::to_string_pretty(&snapshot)
            .map_err(io::Error::other)
            .and_then(|json| std::fs::write(path, json))
        {
            Ok(()) => format!("Saved snapshot to {path}"),
            Err(e) => format!("Save failed: {e}"),
        };

        self.events.push_back(QueryRow {
            time: now.format("%H:%M:%S%.3f").to_string(),
            instant: Instant::now(),
            conn_id: 0,
            latency: String::new(),
            raw_sql: None,
            rows_suffix: String::new(),
            display: message,
            style: Style::default().fg(Color::Cyan),
        });

        if self.auto_scroll {
            self.scroll_to_bottom();
        }
    }

    fn import_from_path(&mut self, path: &str) {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                self.push_status_message(format!("Import failed: {e}"));
                return;
            }
        };

        let snapshot: Snapshot = match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                self.push_status_message(format!("Import failed: invalid JSON: {e}"));
                return;
            }
        };

        // Reconstruct latency buckets
        let latency_buckets = [
            snapshot.latency_buckets.under_1ms,
            snapshot.latency_buckets.ms_1_5,
            snapshot.latency_buckets.ms_5_10,
            snapshot.latency_buckets.ms_10_50,
            snapshot.latency_buckets.ms_50_100,
            snapshot.latency_buckets.over_100ms,
        ];

        // Reconstruct fingerprint aggregates from top_queries
        let mut fingerprints = HashMap::new();
        for q in &snapshot.top_queries {
            let total_duration = Duration::from_secs_f64(q.avg_ms * q.count as f64 / 1000.0);
            fingerprints.insert(q.fingerprint.clone(), QueryAggregates {
                fingerprint: q.fingerprint.clone(),
                count: q.count,
                total_duration,
                min_duration: Duration::from_secs_f64(q.min_ms / 1000.0),
                max_duration: Duration::from_secs_f64(q.max_ms / 1000.0),
            });
        }

        let stats = FrozenStats {
            fingerprints,
            latency_buckets,
            total_queries: snapshot.total_queries,
            total_errors: snapshot.total_errors,
            active_connections: snapshot.active_connections,
            first_query_at: None,
        };

        // Reconstruct event rows
        let now = Instant::now();
        let events: VecDeque<QueryRow> = snapshot.recent_events.into_iter().map(|ev| {
            let msg = &ev.message;

            if msg.starts_with("ERR ") {
                QueryRow {
                    time: ev.time,
                    instant: now,
                    conn_id: ev.conn_id,
                    latency: ev.latency,
                    raw_sql: None,
                    rows_suffix: String::new(),
                    display: msg.clone(),
                    style: Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                }
            } else if msg.starts_with("++ ") || msg.starts_with("-- ") {
                QueryRow {
                    time: ev.time,
                    instant: now,
                    conn_id: ev.conn_id,
                    latency: ev.latency,
                    raw_sql: None,
                    rows_suffix: String::new(),
                    display: msg.clone(),
                    style: Style::default().fg(Color::DarkGray),
                }
            } else if msg.starts_with("WARN:") {
                QueryRow {
                    time: ev.time,
                    instant: now,
                    conn_id: ev.conn_id,
                    latency: ev.latency,
                    raw_sql: None,
                    rows_suffix: String::new(),
                    display: msg.clone(),
                    style: Style::default().fg(Color::Yellow),
                }
            } else {
                // Query event — split trailing " [N]" into rows_suffix
                let (sql, rows_suffix) = if let Some(bracket_pos) = msg.rfind(" [") {
                    if msg.ends_with(']') {
                        (msg[..bracket_pos].to_string(), msg[bracket_pos..].to_string())
                    } else {
                        (msg.clone(), String::new())
                    }
                } else {
                    (msg.clone(), String::new())
                };

                // Parse latency for style
                let ms: f64 = ev.latency.trim_end_matches("ms").parse().unwrap_or(0.0);
                let style = latency_style(ms, self.threshold_ms);

                QueryRow {
                    time: ev.time,
                    instant: now,
                    conn_id: ev.conn_id,
                    latency: ev.latency,
                    raw_sql: Some(sql),
                    rows_suffix,
                    display: String::new(),
                    style,
                }
            }
        }).collect();

        // Extract filename for tab label
        let label = std::path::Path::new(path)
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.to_string());

        self.frozen_tabs.push(FrozenTab {
            label,
            events,
            stats,
            scroll_offset: 0,
            auto_scroll: true,
            show_fingerprints: false,
        });
        self.active_tab = self.frozen_tabs.len(); // switch to new tab

        self.push_status_message(format!("Imported snapshot from {path}"));
    }

    fn push_status_message(&mut self, message: String) {
        let now = chrono::Local::now();
        self.events.push_back(QueryRow {
            time: now.format("%H:%M:%S%.3f").to_string(),
            instant: Instant::now(),
            conn_id: 0,
            latency: String::new(),
            raw_sql: None,
            rows_suffix: String::new(),
            display: message,
            style: Style::default().fg(Color::Cyan),
        });
        if self.auto_scroll {
            self.scroll_to_bottom();
        }
    }

    fn draw(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let has_tabs = !self.frozen_tabs.is_empty();

        // Layout: [tab_bar(1)?] + header(1) + query table (flex) + bottom panels (11) + footer(1)
        let main_chunks = if has_tabs {
            Layout::vertical([
                Constraint::Length(1), // tab bar
                Constraint::Length(1), // header
                Constraint::Min(10),   // query table
                Constraint::Length(11), // bottom panels
                Constraint::Length(1), // footer
            ])
            .split(area)
        } else {
            Layout::vertical([
                Constraint::Length(0), // no tab bar
                Constraint::Length(1),
                Constraint::Min(10),
                Constraint::Length(11),
                Constraint::Length(1),
            ])
            .split(area)
        };

        if has_tabs {
            self.draw_tab_bar(frame, main_chunks[0]);
        }

        // Build DrawContext for the active tab
        if self.active_tab == 0 {
            let qps = self.stats.qps();
            let mut ctx = DrawContext {
                events: &self.events,
                fingerprints: &self.stats.fingerprints,
                latency_buckets: &self.stats.latency_buckets,
                total_queries: self.stats.total_queries,
                total_errors: self.stats.total_errors,
                active_connections: self.stats.active_connections,
                first_query_at: self.stats.first_query_at,
                scroll_offset: &mut self.scroll_offset,
                auto_scroll: self.auto_scroll,
                show_fingerprints: self.show_fingerprints,
                is_frozen: false,
                qps: Some(qps),
            };
            Self::draw_header_ctx(frame, main_chunks[1], &ctx, self.listen_port, &self.upstream, self.paused);
            Self::draw_query_table_ctx(frame, main_chunks[2], &mut ctx);
            Self::draw_bottom_panels_ctx(frame, main_chunks[3], &ctx);
        } else if let Some(tab) = self.frozen_tabs.get_mut(self.active_tab - 1) {
            let mut ctx = DrawContext {
                events: &tab.events,
                fingerprints: &tab.stats.fingerprints,
                latency_buckets: &tab.stats.latency_buckets,
                total_queries: tab.stats.total_queries,
                total_errors: tab.stats.total_errors,
                active_connections: tab.stats.active_connections,
                first_query_at: tab.stats.first_query_at,
                scroll_offset: &mut tab.scroll_offset,
                auto_scroll: tab.auto_scroll,
                show_fingerprints: tab.show_fingerprints,
                is_frozen: true,
                qps: None,
            };
            Self::draw_header_ctx(frame, main_chunks[1], &ctx, self.listen_port, &self.upstream, false);
            Self::draw_query_table_ctx(frame, main_chunks[2], &mut ctx);
            Self::draw_bottom_panels_ctx(frame, main_chunks[3], &ctx);
        }

        self.draw_footer(frame, main_chunks[4]);

        // Draw prompt overlay last (on top of everything)
        if !matches!(self.input_mode, InputMode::Normal) {
            self.draw_prompt(frame, area);
        }
    }

    fn draw_tab_bar(&self, frame: &mut Frame, area: Rect) {
        let active = Style::default().bg(Color::White).fg(Color::Black).add_modifier(Modifier::BOLD);
        let inactive = Style::default().fg(Color::DarkGray);

        let mut spans = vec![Span::styled(
            " Live ",
            if self.active_tab == 0 { active } else { inactive },
        )];

        for (idx, tab) in self.frozen_tabs.iter().enumerate() {
            spans.push(Span::raw(" "));
            let style = if self.active_tab == idx + 1 { active } else { inactive };
            spans.push(Span::styled(format!(" {} ", tab.label), style));
        }

        spans.push(Span::styled("    Tab:switch  x:close", inactive));

        let para = Paragraph::new(Line::from(spans));
        frame.render_widget(para, area);
    }

    fn draw_header_ctx(frame: &mut Frame, area: Rect, ctx: &DrawContext, listen_port: u16, upstream: &str, paused: bool) {
        let qps_str = ctx.qps.map(|q| format!("{q}")).unwrap_or_else(|| "—".into());
        let frozen_str = if ctx.is_frozen { " [FROZEN]" } else { "" };
        let paused_str = if paused { " [PAUSED]" } else { "" };

        let header = format!(
            " dbprobe ── :{} → {} ── conns: {} ── qps: {} ── total: {} ── errs: {}{}{} ",
            listen_port, upstream, ctx.active_connections, qps_str,
            ctx.total_queries, ctx.total_errors, frozen_str, paused_str,
        );

        let style = Style::default().bg(Color::Blue).fg(Color::White).add_modifier(Modifier::BOLD);
        let para = Paragraph::new(header).style(style);
        frame.render_widget(para, area);
    }

    fn draw_query_table_ctx(frame: &mut Frame, area: Rect, ctx: &mut DrawContext) {
        let inner_height = area.height.saturating_sub(3) as usize; // borders + header row

        // Clamp scroll offset
        let max_scroll = ctx.events.len().saturating_sub(inner_height);
        if *ctx.scroll_offset > max_scroll {
            *ctx.scroll_offset = max_scroll;
        }

        let visible_start = *ctx.scroll_offset;
        let visible_end = (visible_start + inner_height).min(ctx.events.len());

        let show_fp = ctx.show_fingerprints;
        let first_instant = ctx.first_query_at;
        let rows: Vec<Row> = ctx.events
            .iter()
            .skip(visible_start)
            .take(visible_end - visible_start)
            .map(|row| {
                let text = match &row.raw_sql {
                    Some(sql) => {
                        let s = if show_fp { crate::fingerprint::fingerprint(sql) } else { sql.clone() };
                        format!("{s}{}", row.rows_suffix)
                    }
                    None => row.display.clone(),
                };
                let elapsed = first_instant
                    .and_then(|f| row.instant.checked_duration_since(f))
                    .map(|d| {
                        let ms = d.as_millis();
                        if ms < 10_000 {
                            format!("{ms}ms")
                        } else {
                            format!("{:.1}s", d.as_secs_f64())
                        }
                    })
                    .unwrap_or_default();
                Row::new(vec![
                    Cell::from(row.time.clone()),
                    Cell::from(format!("{}", row.conn_id)),
                    Cell::from(row.latency.clone()),
                    Cell::from(elapsed),
                    Cell::from(text),
                ])
                .style(row.style)
            })
            .collect();

        let scroll_indicator = if ctx.auto_scroll {
            "AUTO".to_string()
        } else {
            format!("{}/{}", *ctx.scroll_offset + inner_height, ctx.events.len())
        };

        let table = Table::new(
            rows,
            [
                Constraint::Length(12),
                Constraint::Length(5),
                Constraint::Length(10),
                Constraint::Length(8),
                Constraint::Min(30),
            ],
        )
        .header(
            Row::new(vec!["TIME", "CONN", "LATENCY", "ELAPSED", "QUERY"])
                .style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan))
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Queries [{scroll_indicator}] "))
        );

        frame.render_widget(table, area);
    }

    fn draw_bottom_panels_ctx(frame: &mut Frame, area: Rect, ctx: &DrawContext) {
        let chunks = Layout::horizontal([
            Constraint::Percentage(40),
            Constraint::Percentage(60),
        ])
        .split(area);

        Self::draw_latency_histogram_ctx(frame, chunks[0], ctx);
        Self::draw_top_queries_ctx(frame, chunks[1], ctx);
    }

    fn draw_latency_histogram_ctx(frame: &mut Frame, area: Rect, ctx: &DrawContext) {
        let labels = ["<1ms", "1-5ms", "5-10ms", "10-50ms", "50-100ms", ">100ms"];
        let data: Vec<(&str, u64)> = labels
            .iter()
            .zip(ctx.latency_buckets.iter())
            .map(|(&label, &count)| (label, count))
            .collect();

        let chart = BarChart::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Latency Distribution ")
            )
            .data(&data)
            .bar_width(7)
            .bar_gap(1)
            .bar_style(Style::default().fg(Color::Green))
            .value_style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));

        frame.render_widget(chart, area);
    }

    fn draw_top_queries_ctx(frame: &mut Frame, area: Rect, ctx: &DrawContext) {
        let mut top: Vec<_> = ctx.fingerprints.values().cloned().collect();
        top.sort_unstable_by(|a, b| b.total_duration.cmp(&a.total_duration));
        top.truncate(5);
        let inner_width = area.width.saturating_sub(2) as usize;

        let mut rows: Vec<Row> = top
            .iter()
            .map(|q: &QueryAggregates| {
                let avg_ms = if q.count > 0 {
                    q.total_duration.as_secs_f64() * 1000.0 / q.count as f64
                } else {
                    0.0
                };
                let fp_max_len = inner_width.saturating_sub(22);
                let fp = if q.fingerprint.len() > fp_max_len {
                    format!("{}..", &q.fingerprint[..fp_max_len.saturating_sub(2)])
                } else {
                    q.fingerprint.clone()
                };
                Row::new(vec![
                    Cell::from(fp),
                    Cell::from(format!("{}", q.count)),
                    Cell::from(format!("{avg_ms:.1}ms")),
                ])
            })
            .collect();

        // Total row
        if ctx.total_queries > 0 {
            let total_count = ctx.total_queries;
            let total_dur: Duration = ctx.fingerprints.values()
                .map(|q| q.total_duration)
                .sum();
            let total_avg = total_dur.as_secs_f64() * 1000.0 / total_count as f64;
            let unique = ctx.fingerprints.len();
            rows.push(
                Row::new(vec![
                    Cell::from(format!("TOTAL ({unique} unique)")),
                    Cell::from(format!("{total_count}")),
                    Cell::from(format!("{total_avg:.1}ms")),
                ])
                .style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Yellow))
            );
        }

        let table = Table::new(
            rows,
            [
                Constraint::Min(20),
                Constraint::Length(8),
                Constraint::Length(10),
            ],
        )
        .header(
            Row::new(vec!["QUERY", "COUNT", "AVG"])
                .style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan))
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Top Queries (by total time) ")
        );

        frame.render_widget(table, area);
    }

    fn draw_prompt(&self, frame: &mut Frame, area: Rect) {
        let (title, buffer, cursor) = match &self.input_mode {
            InputMode::SavePrompt { buffer, cursor } => ("Save As", buffer.as_str(), *cursor),
            InputMode::ImportPrompt { buffer, cursor } => ("Import File", buffer.as_str(), *cursor),
            InputMode::Normal => return,
        };

        let width = 50u16.min(area.width.saturating_sub(4));
        let x = area.x + (area.width.saturating_sub(width)) / 2;
        let y = area.y + area.height / 2 - 2;
        let prompt_area = Rect::new(x, y, width, 4);

        // Clear background
        let clear = ratatui::widgets::Clear;
        frame.render_widget(clear, prompt_area);

        let inner_width = width.saturating_sub(4) as usize;
        // Scroll the visible portion if buffer is wider than the box
        let visible_start = cursor.saturating_sub(inner_width);
        let visible_end = (visible_start + inner_width).min(buffer.len());
        let visible_text = &buffer[visible_start..visible_end];

        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" {title} "))
            .title_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));

        let inner = block.inner(prompt_area);
        frame.render_widget(block, prompt_area);

        let input_line = Paragraph::new(visible_text);
        frame.render_widget(input_line, Rect::new(inner.x, inner.y, inner.width, 1));

        let hint = Paragraph::new("Enter:confirm  Esc:cancel")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(hint, Rect::new(inner.x, inner.y + 1, inner.width, 1));

        // Position cursor
        let cursor_x = inner.x + (cursor - visible_start) as u16;
        frame.set_cursor_position((cursor_x, inner.y));
    }

    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let help = if self.frozen_tabs.is_empty() {
            " q:quit  j/k:scroll  G:bottom  g:top  f:fingerprint  p:pause  r:reset  s:save  i:import  t:new-tab ".to_string()
        } else {
            " q:quit  j/k:scroll  G:bottom  g:top  f:fingerprint  p:pause  r:reset  s:save  i:import  t:new-tab  Tab:switch  x:close ".to_string()
        };
        let style = Style::default().fg(Color::DarkGray);
        let para = Paragraph::new(help).style(style);
        frame.render_widget(para, area);
    }
}

fn latency_style(ms: f64, threshold_ms: u64) -> Style {
    if ms >= threshold_ms as f64 {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    } else if ms >= 50.0 {
        Style::default().fg(Color::Red)
    } else if ms >= 5.0 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Green)
    }
}

/// Restore terminal state. Called on both clean exit and error paths.
fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) {
    let _ = disable_raw_mode();
    let _ = terminal.backend_mut().execute(LeaveAlternateScreen);
    let _ = terminal.show_cursor();
}

/// Run the TUI. This takes over the terminal.
/// Receives ProxyMessages via the channel, processes stats internally.
pub async fn run_tui(
    mut rx: mpsc::UnboundedReceiver<ProxyMessage>,
    listen_port: u16,
    upstream: String,
    threshold_ms: u64,
) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_tui_loop(&mut terminal, &mut rx, listen_port, upstream, threshold_ms).await;

    // Always restore terminal, even if the loop returned an error.
    restore_terminal(&mut terminal);

    result
}

async fn run_tui_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    rx: &mut mpsc::UnboundedReceiver<ProxyMessage>,
    listen_port: u16,
    upstream: String,
    threshold_ms: u64,
) -> anyhow::Result<()> {
    let mut app = TuiApp::new(listen_port, upstream, threshold_ms);

    loop {
        terminal.draw(|frame| app.draw(frame))?;

        // Poll for crossterm events
        if event::poll(Duration::from_millis(10))? {
            if let Event::Key(key) = event::read()? {
                app.handle_key(key.code, key.modifiers);
                if app.should_quit {
                    break;
                }
            }
        }

        // Drain proxy messages (non-blocking)
        loop {
            match rx.try_recv() {
                Ok(msg) => {
                    match msg {
                        ProxyMessage::ConnectionOpened { conn_id } => {
                            let event = app.stats.connection_opened(conn_id);
                            app.push_event(&event);
                        }
                        ProxyMessage::ConnectionClosed { conn_id } => {
                            if let Some(event) = app.stats.connection_dropped(conn_id) {
                                app.push_event(&event);
                            }
                        }
                        ProxyMessage::Event { conn_id, event } => {
                            if let Some(display_event) = app.stats.process_event(conn_id, event) {
                                app.push_event(&display_event);
                            }
                        }
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    app.should_quit = true;
                    break;
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

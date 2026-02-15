use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, BarChart};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::proxy::ProxyMessage;
use crate::stats::{QueryAggregates, StatsCollector};
use super::{DisplayEvent, DisplayEventKind};

#[derive(Serialize)]
struct Snapshot {
    timestamp: String,
    total_queries: u64,
    total_errors: u64,
    active_connections: u64,
    latency_buckets: LatencyBuckets,
    top_queries: Vec<SnapshotQuery>,
    recent_events: Vec<SnapshotEvent>,
}

#[derive(Serialize)]
struct LatencyBuckets {
    under_1ms: u64,
    ms_1_5: u64,
    ms_5_10: u64,
    ms_10_50: u64,
    ms_50_100: u64,
    over_100ms: u64,
}

#[derive(Serialize)]
struct SnapshotQuery {
    fingerprint: String,
    count: u64,
    avg_ms: f64,
    min_ms: f64,
    max_ms: f64,
}

#[derive(Serialize)]
struct SnapshotEvent {
    time: String,
    conn_id: u64,
    latency: String,
    message: String,
}

const MAX_EVENTS: usize = 10_000;

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

    fn handle_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        match code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => self.should_quit = true,
            KeyCode::Char('j') | KeyCode::Down => {
                self.auto_scroll = false;
                self.scroll_offset = self.scroll_offset.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.auto_scroll = false;
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
            }
            KeyCode::Char('G') | KeyCode::End => {
                self.auto_scroll = true;
                self.scroll_to_bottom();
            }
            KeyCode::Char('g') | KeyCode::Home => {
                self.auto_scroll = false;
                self.scroll_offset = 0;
            }
            KeyCode::Char('f') => {
                self.show_fingerprints = !self.show_fingerprints;
            }
            KeyCode::Char('p') => {
                self.paused = !self.paused;
            }
            KeyCode::Char('r') => {
                self.stats.reset();
                self.events.clear();
                self.scroll_offset = 0;
                self.auto_scroll = true;
            }
            KeyCode::Char('s') => {
                self.save_snapshot();
            }
            KeyCode::PageDown => {
                self.auto_scroll = false;
                self.scroll_offset = self.scroll_offset.saturating_add(20);
            }
            KeyCode::PageUp => {
                self.auto_scroll = false;
                self.scroll_offset = self.scroll_offset.saturating_sub(20);
            }
            _ => {}
        }
    }

    fn save_snapshot(&mut self) {
        let now = chrono::Local::now();
        let filename = format!("dbprobe-{}.json", now.format("%Y%m%dT%H%M%S"));

        let b = &self.stats.latency_buckets;
        let snapshot = Snapshot {
            timestamp: now.to_rfc3339(),
            total_queries: self.stats.total_queries,
            total_errors: self.stats.total_errors,
            active_connections: self.stats.active_connections,
            latency_buckets: LatencyBuckets {
                under_1ms: b[0],
                ms_1_5: b[1],
                ms_5_10: b[2],
                ms_10_50: b[3],
                ms_50_100: b[4],
                over_100ms: b[5],
            },
            top_queries: self.stats.top_queries(20).into_iter().map(|q| {
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
            recent_events: self.events.iter().map(|row| {
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
            .and_then(|json| std::fs::write(&filename, json))
        {
            Ok(()) => format!("Saved snapshot to {filename}"),
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

    fn draw(&mut self, frame: &mut Frame) {
        let area = frame.area();

        // Layout: header(1) + query table (flex) + bottom panels (11) + footer(1)
        let main_chunks = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(11),
            Constraint::Length(1),
        ])
        .split(area);

        self.draw_header(frame, main_chunks[0]);
        self.draw_query_table(frame, main_chunks[1]);
        self.draw_bottom_panels(frame, main_chunks[2]);
        self.draw_footer(frame, main_chunks[3]);
    }

    fn draw_header(&mut self, frame: &mut Frame, area: Rect) {
        let qps = self.stats.qps();
        let conns = self.stats.active_connections;
        let errs = self.stats.total_errors;
        let total = self.stats.total_queries;
        let paused = if self.paused { " [PAUSED]" } else { "" };

        let header = format!(
            " dbprobe ── :{} → {} ── conns: {} ── qps: {} ── total: {} ── errs: {}{} ",
            self.listen_port, self.upstream, conns, qps, total, errs, paused,
        );

        let style = Style::default().bg(Color::Blue).fg(Color::White).add_modifier(Modifier::BOLD);
        let para = Paragraph::new(header).style(style);
        frame.render_widget(para, area);
    }

    fn draw_query_table(&mut self, frame: &mut Frame, area: Rect) {
        let inner_height = area.height.saturating_sub(2) as usize; // borders

        // Clamp scroll offset
        let max_scroll = self.events.len().saturating_sub(inner_height);
        if self.scroll_offset > max_scroll {
            self.scroll_offset = max_scroll;
        }

        let visible_start = self.scroll_offset;
        let visible_end = (visible_start + inner_height).min(self.events.len());

        let show_fp = self.show_fingerprints;
        let first_instant = self.stats.first_query_at;
        let rows: Vec<Row> = self.events
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

        let scroll_indicator = if self.auto_scroll {
            "AUTO"
        } else {
            &format!("{}/{}", self.scroll_offset + inner_height, self.events.len())
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

    fn draw_bottom_panels(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::horizontal([
            Constraint::Percentage(40),
            Constraint::Percentage(60),
        ])
        .split(area);

        self.draw_latency_histogram(frame, chunks[0]);
        self.draw_top_queries(frame, chunks[1]);
    }

    fn draw_latency_histogram(&self, frame: &mut Frame, area: Rect) {
        let labels = ["<1ms", "1-5ms", "5-10ms", "10-50ms", "50-100ms", ">100ms"];
        let data: Vec<(&str, u64)> = labels
            .iter()
            .zip(self.stats.latency_buckets.iter())
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

    fn draw_top_queries(&self, frame: &mut Frame, area: Rect) {
        let top = self.stats.top_queries(5);
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
        if self.stats.total_queries > 0 {
            let total_count: u64 = self.stats.total_queries;
            let total_dur: Duration = self.stats.fingerprints.values()
                .map(|q| q.total_duration)
                .sum();
            let total_avg = total_dur.as_secs_f64() * 1000.0 / total_count as f64;
            let unique = self.stats.fingerprints.len();
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

    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let help = " q:quit  j/k:scroll  G:bottom  g:top  f:fingerprint  p:pause  r:reset  s:save ";
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

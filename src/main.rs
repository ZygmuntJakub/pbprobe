mod fingerprint;
mod output;
mod protocol;
mod proxy;
mod stats;

use std::io::IsTerminal;

use clap::{Parser, ValueEnum};
use tokio::sync::mpsc;
use tracing::info;

use output::raw::RawSink;
use output::OutputSink;
use proxy::ProxyMessage;
use stats::StatsCollector;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Raw,
    Tui,
}

#[derive(Parser, Debug)]
#[command(name = "dbprobe", about = "Lightweight database wire protocol interceptor")]
struct Cli {
    /// Local port to listen on
    #[arg(short = 'l', long = "listen", default_value = "5433")]
    listen_port: u16,

    /// Upstream database address (host:port)
    #[arg(short = 'u', long = "upstream", default_value = "localhost:5432")]
    upstream: String,

    /// Output mode: raw (stdout) or tui (dashboard). Auto-detected if omitted.
    #[arg(short = 'm', long = "mode")]
    mode: Option<Mode>,

    /// Highlight queries slower than this threshold (ms)
    #[arg(short = 't', long = "threshold", default_value = "100")]
    threshold_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let mode = cli.mode.unwrap_or_else(|| {
        if std::io::stdout().is_terminal() {
            Mode::Tui
        } else {
            Mode::Raw
        }
    });

    let use_tui = matches!(mode, Mode::Tui);

    if !use_tui {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("dbprobe=info".parse().unwrap()),
            )
            .with_target(false)
            .init();

        info!(
            "dbprobe starting — listening on :{}, forwarding to {}",
            cli.listen_port, cli.upstream
        );
    }

    let (tx, rx) = mpsc::channel::<ProxyMessage>(1024);

    let listen_addr = format!("0.0.0.0:{}", cli.listen_port);
    let upstream_addr = cli.upstream.clone();

    let proxy_handle = tokio::spawn(async move {
        if let Err(e) = proxy::run_proxy(&listen_addr, upstream_addr, tx).await {
            tracing::error!("Proxy error: {e}");
        }
    });

    if use_tui {
        let tui_handle = tokio::spawn(output::tui::run_tui(
            rx,
            cli.listen_port,
            cli.upstream.clone(),
            cli.threshold_ms,
        ));

        tokio::select! {
            result = tui_handle => {
                if let Err(e) = result {
                    eprintln!("TUI error: {e}");
                }
            }
            _ = proxy_handle => {}
        }
    } else {
        let event_handle = tokio::spawn(run_raw_mode(rx));

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutting down...");
            }
            _ = proxy_handle => {}
            _ = event_handle => {}
        }
    }

    Ok(())
}

async fn run_raw_mode(mut rx: mpsc::Receiver<ProxyMessage>) {
    let mut stats = StatsCollector::new();
    let mut sink = RawSink::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            ProxyMessage::ConnectionOpened { conn_id } => {
                let event = stats.connection_opened(conn_id);
                sink.handle_event(&event);
            }
            ProxyMessage::ConnectionClosed { conn_id } => {
                if let Some(event) = stats.connection_dropped(conn_id) {
                    sink.handle_event(&event);
                }
            }
            ProxyMessage::Event { conn_id, event } => {
                if let Some(display_event) = stats.process_event(conn_id, event) {
                    sink.handle_event(&display_event);
                }
            }
        }
    }

    sink.shutdown();
}

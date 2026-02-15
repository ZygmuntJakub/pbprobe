<p align="center">
  <h1 align="center">dbprobe</h1>
  <p align="center">
    Real-time database query interceptor with a live TUI dashboard
  </p>
  <p align="center">
    <a href="#installation">Installation</a> &middot;
    <a href="#usage">Usage</a> &middot;
    <a href="#features">Features</a> &middot;
    <a href="ROADMAP.md">Roadmap</a>
  </p>
</p>

---

**dbprobe** is a lightweight TCP proxy that sits between your application and PostgreSQL, parsing every query in real-time. It shows a live dashboard with latency histograms, query aggregation by fingerprint, slow query highlighting, and error detection — all in your terminal.

```
app :5433 ──→ dbprobe ──→ postgres :5432
    ←──────          ←────
```

Zero database extensions. Zero config changes. Just point your app at a different port.

<p align="center">
  <video src="https://github.com/user-attachments/assets/eea2e540-dd0b-4b3d-840b-d1b687d5667c" width="800" autoplay loop muted playsinline></video>
</p>

## Features

- **Transparent proxy** — forward-first architecture; parsing never adds latency to your queries
- **Live query stream** — every query with latency, row counts, errors, color-coded by speed
- **Extended query protocol** — full support for Parse/Bind/Execute pipelines (prepared statements, ORMs, connection pools)
- **Latency histogram** — distribution across 6 buckets (< 1 ms to > 100 ms)
- **Top queries** — aggregated by SQL fingerprint, sorted by total time (like `pg_stat_statements`)
- **SQL fingerprinting** — normalizes literals (`'alice'` -> `$S`, `42` -> `$N`, `IN (1,2,3)` -> `IN ($...)`)
- **Error detection** — captures SQLSTATE codes and error messages in real-time
- **Slow query highlighting** — configurable threshold, queries above it glow red
- **Two output modes** — interactive TUI dashboard or pipe-friendly raw text
- **Auto-detection** — TUI when connected to a terminal, raw when piped

## Installation

### From source

```bash
cargo install --path .
```

### Build from git

```bash
git clone https://github.com/youruser/dbprobe.git
cd dbprobe
cargo build --release
# Binary at ./target/release/dbprobe
```

**Requirements**: Rust 1.70+ (edition 2021)

## Usage

```bash
# Start with defaults (listen :5433, upstream localhost:5432, TUI mode)
dbprobe

# Custom ports
dbprobe -l 6433 -u postgres-primary.internal:5432

# Slow query threshold at 50ms (default: 100ms)
dbprobe -t 50

# Raw mode — grep-able, pipe-friendly
dbprobe --mode raw

# Pipe to a file while watching
dbprobe --mode raw | tee queries.log
```

Then point your application at the proxy:

```bash
# psql
psql -h localhost -p 5433 -U postgres -d mydb

# Connection string
postgresql://postgres:password@localhost:5433/mydb
```

### CLI Options

```
Usage: dbprobe [OPTIONS]

Options:
  -l, --listen <PORT>        Local port to listen on [default: 5433]
  -u, --upstream <ADDR>      Upstream database address [default: localhost:5432]
  -m, --mode <MODE>          Output mode: tui or raw [default: auto-detect]
  -t, --threshold <MS>       Highlight queries slower than this [default: 100]
  -h, --help                 Print help
```

## How It Works

dbprobe speaks the [PostgreSQL v3.0 wire protocol](https://www.postgresql.org/docs/current/protocol.html). It intercepts TCP traffic, parses messages from both client and server, and correlates query starts with their completions to compute per-query latency.

### Protocol Support

| Protocol Feature | Status |
|-----------------|--------|
| Simple query (`Q` message) | Supported |
| Extended query (Parse/Bind/Execute) | Supported |
| Pipelined queries | Supported |
| SSL negotiation (intercepted, replies `N`) | Supported |
| CommandComplete row counts | Supported |
| ErrorResponse with SQLSTATE | Supported |
| Transaction state tracking | Supported |
| COPY protocol | Not yet |
| Streaming replication | Not supported |

### What We Measure

Latency is measured from when dbprobe sees the query (Execute or Query message) to when the server returns CommandComplete. This includes:

- PostgreSQL planning + execution time
- Network round-trip between dbprobe and PostgreSQL
- Any lock wait or queueing inside PostgreSQL

This is **client-perceived latency** — the number your application actually experiences.

## SSL / TLS

dbprobe intercepts PostgreSQL's SSL negotiation and responds with `N` (no SSL), forcing plaintext communication. Most clients (libpq, JDBC, node-postgres) with `sslmode=prefer` (the default) will fall back to plaintext automatically.

If your client uses `sslmode=require`, set it to `sslmode=prefer` or `sslmode=disable` when connecting through dbprobe.

**Note**: Connections through dbprobe are unencrypted. For production use, ensure dbprobe runs on localhost or a trusted network.

## Development

```bash
# Start test PostgreSQL
docker compose up -d

# Run with logging
RUST_LOG=debug cargo run -- --mode raw

# Run tests
cargo test

# Lint (zero warnings policy)
cargo clippy -- -D warnings
```

## License

[MIT](LICENSE)

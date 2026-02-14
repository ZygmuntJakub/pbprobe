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

## TUI Dashboard

The dashboard has four sections:

| Section | Description |
|---------|-------------|
| **Header** | Port, upstream, active connections, QPS, total queries, errors |
| **Query log** | Scrollable table of every query with time, connection, latency, SQL |
| **Latency histogram** | Bar chart across 6 latency buckets |
| **Top queries** | Fingerprinted queries sorted by total time with count and average |

### Keybindings

| Key | Action |
|-----|--------|
| `q` / `Ctrl-C` | Quit |
| `j` / `k` / `Up` / `Down` | Scroll query log |
| `PgUp` / `PgDn` | Scroll by page |
| `g` / `Home` | Jump to top |
| `G` / `End` | Jump to bottom (re-enable auto-scroll) |
| `f` | Toggle fingerprint view (normalize SQL) |
| `p` | Pause / resume event stream |

### Raw Mode Output

```
14:23:01.456 [conn:1]      0.8ms  SELECT * FROM users WHERE id = 42 [1 rows]
14:23:01.458 [conn:1]     23.5ms  UPDATE orders SET status = 'shipped' WHERE id = 99 [1 rows]
14:23:01.461 [conn:2]    156.2ms  SELECT * FROM products JOIN categories... [342 rows]
14:23:01.462 [conn:1]      1.2ms  COMMIT
14:23:01.500 [conn:3]            ERR 42P01: relation "nonexistent" does not exist
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

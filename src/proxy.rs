use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::protocol::postgres::PostgresParser;
use crate::protocol::{Direction, ProtoEvent, ProtocolParser};

pub enum ProxyMessage {
    Event {
        conn_id: u64,
        event: ProtoEvent,
    },
    ConnectionOpened {
        conn_id: u64,
    },
    ConnectionClosed {
        conn_id: u64,
    },
}

static CONN_COUNTER: AtomicU64 = AtomicU64::new(1);

pub async fn run_proxy(
    listen_addr: &str,
    upstream_addr: String,
    tx: mpsc::Sender<ProxyMessage>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    info!("Listening on {listen_addr}, forwarding to {upstream_addr}");

    loop {
        let (client_stream, client_addr) = listener.accept().await?;
        let conn_id = CONN_COUNTER.fetch_add(1, Ordering::Relaxed);
        let upstream_addr = upstream_addr.clone();
        let tx = tx.clone();

        debug!("New connection {conn_id} from {client_addr}");
        let _ = tx.send(ProxyMessage::ConnectionOpened { conn_id }).await;

        tokio::spawn(async move {
            if let Err(e) = handle_connection(conn_id, client_stream, &upstream_addr, tx.clone()).await {
                warn!("Connection {conn_id} error: {e}");
            }
            let _ = tx.send(ProxyMessage::ConnectionClosed { conn_id }).await;
            debug!("Connection {conn_id} closed");
        });
    }
}

async fn handle_connection(
    conn_id: u64,
    client_stream: TcpStream,
    upstream_addr: &str,
    tx: mpsc::Sender<ProxyMessage>,
) -> anyhow::Result<()> {
    let upstream_stream = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        TcpStream::connect(upstream_addr),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            error!("Failed to connect to upstream {upstream_addr}: {e}");
            return Err(e.into());
        }
        Err(_) => {
            error!("Timeout connecting to upstream {upstream_addr}");
            return Err(anyhow::anyhow!("upstream connect timeout"));
        }
    };

    let (client_read, client_write) = client_stream.into_split();
    let (upstream_read, upstream_write) = upstream_stream.into_split();

    // std::sync::Mutex is correct here: the critical section is pure CPU parsing (~us),
    // never crosses an await point, and avoids the overhead of tokio's async Mutex.
    let parser = Arc::new(Mutex::new(
        Box::new(PostgresParser::new()) as Box<dyn ProtocolParser>
    ));

    let (intercept_tx, mut intercept_rx) = mpsc::channel::<Vec<u8>>(4);
    let (client_write_tx, mut client_write_rx) = mpsc::channel::<Bytes>(256);

    let client_writer_handle = tokio::spawn(async move {
        let mut writer = client_write;
        loop {
            tokio::select! {
                Some(data) = client_write_rx.recv() => {
                    if writer.write_all(&data).await.is_err() {
                        break;
                    }
                }
                Some(data) = intercept_rx.recv() => {
                    if writer.write_all(&data).await.is_err() {
                        break;
                    }
                }
                else => break,
            }
        }
    });

    let parser_fe = parser.clone();
    let tx_fe = tx.clone();
    let mut frontend_handle = tokio::spawn(async move {
        relay_frontend(
            client_read,
            upstream_write,
            parser_fe,
            tx_fe,
            conn_id,
            intercept_tx,
        )
        .await
    });

    let parser_be = parser.clone();
    let tx_be = tx;
    let mut backend_handle = tokio::spawn(async move {
        relay_backend(
            upstream_read,
            client_write_tx,
            parser_be,
            tx_be,
            conn_id,
        )
        .await
    });

    // Wait for either direction to finish, then clean up both.
    tokio::select! {
        _ = &mut frontend_handle => {}
        _ = &mut backend_handle => {}
    }

    // Abort all remaining tasks so we don't leak them.
    frontend_handle.abort();
    backend_handle.abort();
    client_writer_handle.abort();

    Ok(())
}

async fn relay_frontend(
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    parser: Arc<Mutex<Box<dyn ProtocolParser>>>,
    events_tx: mpsc::Sender<ProxyMessage>,
    conn_id: u64,
    intercept_tx: mpsc::Sender<Vec<u8>>,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 16384];
    let mut parse_buf = BytesMut::with_capacity(16384);

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        parse_buf.extend_from_slice(&buf[..n]);

        // Check for SSL intercept before forwarding.
        // Lock is scoped so the MutexGuard is dropped before any .await.
        let intercept_response = {
            let mut parser = parser.lock().unwrap();
            parser.handle_startup_intercept(&parse_buf, Direction::Frontend)
        };

        if let Some(response) = intercept_response {
            intercept_tx.send(response).await.ok();
            // Consume the SSLRequest from parse buffer
            let length = if parse_buf.len() >= 4 {
                u32::from_be_bytes([parse_buf[0], parse_buf[1], parse_buf[2], parse_buf[3]]) as usize
            } else {
                8
            };
            if parse_buf.len() >= length {
                let _ = parse_buf.split_to(length);
            }
            // If there's leftover data after the SSLRequest, forward it to upstream.
            if !parse_buf.is_empty() {
                writer.write_all(&parse_buf).await?;
            }
        } else {
            writer.write_all(&buf[..n]).await?;
        }

        // Parse events from buffer.
        // Collect events under the lock, then send them after releasing it.
        let events: Vec<ProtoEvent> = {
            let mut parser = parser.lock().unwrap();
            let mut collected = Vec::new();
            while let Some((event, consumed)) = parser.try_parse(&parse_buf, Direction::Frontend) {
                collected.push(event);
                let _ = parse_buf.split_to(consumed);
            }
            collected
        };
        for event in events {
            let _ = events_tx
                .send(ProxyMessage::Event { conn_id, event })
                .await;
        }
    }

    Ok(())
}

async fn relay_backend(
    mut reader: OwnedReadHalf,
    writer_tx: mpsc::Sender<Bytes>,
    parser: Arc<Mutex<Box<dyn ProtocolParser>>>,
    events_tx: mpsc::Sender<ProxyMessage>,
    conn_id: u64,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 16384];
    let mut parse_buf = BytesMut::with_capacity(16384);

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        // Forward immediately to client. Use Bytes to avoid a copy when possible.
        if writer_tx.send(Bytes::copy_from_slice(&buf[..n])).await.is_err() {
            break;
        }

        parse_buf.extend_from_slice(&buf[..n]);

        let events: Vec<ProtoEvent> = {
            let mut parser = parser.lock().unwrap();
            let mut collected = Vec::new();
            while let Some((event, consumed)) = parser.try_parse(&parse_buf, Direction::Backend) {
                collected.push(event);
                let _ = parse_buf.split_to(consumed);
            }
            collected
        };
        for event in events {
            let _ = events_tx
                .send(ProxyMessage::Event { conn_id, event })
                .await;
        }
    }

    Ok(())
}

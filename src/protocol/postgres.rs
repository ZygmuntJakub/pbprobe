use std::collections::HashMap;

use super::{Direction, ProtoEvent, ProtocolParser, TxStatus};
use tracing::{debug, trace, warn};

/// Connection phase state machine for PostgreSQL wire protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ConnPhase {
    /// First message from client — no tag byte.
    AwaitingStartup,
    /// We replied 'N' to SSLRequest, waiting for real StartupMessage.
    AwaitingStartupAfterSslReject,
    /// Authentication in progress — forward everything, wait for ReadyForQuery.
    Authenticating,
    /// Normal traffic — parse queries.
    Ready,
}

const SSL_REQUEST_CODE: u32 = 80877103;
const STARTUP_VERSION_3_0: u32 = 196608;
const CANCEL_REQUEST_CODE: u32 = 80877102;

const MAX_SQL_LEN: usize = 4096;

pub struct PostgresParser {
    phase: ConnPhase,
    /// Prepared statements: stmt_name -> SQL text.
    statements: HashMap<String, String>,
    /// Bound portals: portal_name -> stmt_name.
    portals: HashMap<String, String>,
}

impl PostgresParser {
    pub fn new() -> Self {
        Self {
            phase: ConnPhase::AwaitingStartup,
            statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    /// Try to parse a startup message (no tag byte).
    fn try_parse_startup(&mut self, buf: &[u8]) -> Option<(ProtoEvent, usize)> {
        if buf.len() < 8 {
            return None;
        }

        let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if !(8..=10_000).contains(&length) {
            warn!("Invalid startup message length: {length}, skipping 1 byte");
            return Some((ProtoEvent::Unknown { tag: 0 }, 1));
        }

        if buf.len() < length {
            return None;
        }

        let version = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

        match version {
            SSL_REQUEST_CODE => {
                debug!("SSLRequest detected (should be intercepted)");
                Some((ProtoEvent::Unknown { tag: 0 }, length))
            }
            STARTUP_VERSION_3_0 => {
                debug!("StartupMessage v3.0");
                self.phase = ConnPhase::Authenticating;
                Some((ProtoEvent::Unknown { tag: 0 }, length))
            }
            CANCEL_REQUEST_CODE => {
                debug!("CancelRequest");
                Some((ProtoEvent::Unknown { tag: 0 }, length))
            }
            _ => {
                warn!("Unknown startup version: {version}");
                Some((ProtoEvent::Unknown { tag: 0 }, length))
            }
        }
    }

    /// Try to parse a regular message (with tag byte).
    fn try_parse_regular(
        &mut self,
        buf: &[u8],
        direction: Direction,
    ) -> Option<(ProtoEvent, usize)> {
        if buf.len() < 5 {
            return None;
        }

        let tag = buf[0];
        let raw_length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);

        if raw_length < 4 {
            warn!("Invalid message length {raw_length} for tag '{}'", tag as char);
            return Some((ProtoEvent::Unknown { tag }, 1));
        }

        let total_len = 1 + raw_length as usize; // tag byte + length (which includes itself)
        if buf.len() < total_len {
            return None;
        }

        let payload = &buf[5..total_len];
        let event = self.parse_message(tag, payload, direction);

        Some((event, total_len))
    }

    fn parse_message(&mut self, tag: u8, payload: &[u8], direction: Direction) -> ProtoEvent {
        match (direction, tag) {
            // Frontend: Simple Query
            (Direction::Frontend, b'Q') => {
                let sql = extract_cstring(payload).unwrap_or_default();
                let sql = truncate_sql(&sql);
                trace!("Query: {sql}");
                ProtoEvent::QueryStart { sql }
            }

            // Frontend: Parse (Extended Query Protocol)
            (Direction::Frontend, b'P') => {
                // Format: stmt_name\0 sql\0 param_count(i16) param_types...
                if let Some(name_end) = payload.iter().position(|&b| b == 0) {
                    let stmt_name = String::from_utf8_lossy(&payload[..name_end]).into_owned();
                    let rest = &payload[name_end + 1..];
                    let sql = extract_cstring(rest).unwrap_or_default();
                    let sql = truncate_sql(&sql);
                    trace!("Parse (extended): stmt={stmt_name:?} sql={sql}");
                    self.statements.insert(stmt_name, sql.clone());
                    ProtoEvent::ParseDetected { sql }
                } else {
                    ProtoEvent::Unknown { tag }
                }
            }

            // Frontend: Bind
            (Direction::Frontend, b'B') => {
                // Format: portal_name\0 stmt_name\0 ...
                if let Some(portal_end) = payload.iter().position(|&b| b == 0) {
                    let portal = String::from_utf8_lossy(&payload[..portal_end]).into_owned();
                    let rest = &payload[portal_end + 1..];
                    let stmt = extract_cstring(rest).unwrap_or_default();
                    trace!("Bind: portal={portal:?} stmt={stmt:?}");
                    self.portals.insert(portal, stmt);
                }
                ProtoEvent::Unknown { tag }
            }

            // Frontend: Execute
            (Direction::Frontend, b'E') => {
                // Format: portal_name\0 max_rows(i32)
                let portal = extract_cstring(payload).unwrap_or_default();
                let sql = self.portals.get(&portal)
                    .and_then(|stmt| self.statements.get(stmt))
                    .cloned()
                    .unwrap_or_else(|| format!("<execute portal={portal:?}>"));
                trace!("Execute: portal={portal:?} sql={sql}");
                ProtoEvent::QueryStart { sql }
            }

            // Frontend: Close
            (Direction::Frontend, b'C') => {
                // Format: type_byte ('S' or 'P') name\0
                if !payload.is_empty() {
                    let close_type = payload[0];
                    let name = extract_cstring(&payload[1..]).unwrap_or_default();
                    match close_type {
                        b'S' => { self.statements.remove(&name); }
                        b'P' => { self.portals.remove(&name); }
                        _ => {}
                    }
                    trace!("Close: type={} name={name:?}", close_type as char);
                }
                ProtoEvent::Unknown { tag }
            }

            // Frontend: Sync, Describe, Flush — transparent passthrough
            (Direction::Frontend, b'S') | (Direction::Frontend, b'D') | (Direction::Frontend, b'H') => {
                ProtoEvent::Unknown { tag }
            }

            // Frontend: Terminate
            (Direction::Frontend, b'X') => ProtoEvent::ConnectionClosed,

            // Backend: CommandComplete
            (Direction::Backend, b'C') => {
                let tag_str = extract_cstring(payload).unwrap_or_default();
                let rows = parse_command_tag_rows(&tag_str);
                trace!("CommandComplete: {tag_str} (rows: {rows:?})");
                ProtoEvent::QueryComplete {
                    tag: tag_str,
                    rows,
                }
            }

            // Backend: ErrorResponse
            (Direction::Backend, b'E') => {
                let (severity, code, message) = parse_error_response(payload);
                trace!("Error: {severity} {code} {message}");
                ProtoEvent::QueryError {
                    severity,
                    code,
                    message,
                }
            }

            // Backend: ReadyForQuery
            (Direction::Backend, b'Z') => {
                let status = if payload.is_empty() {
                    TxStatus::Idle
                } else {
                    match payload[0] {
                        b'I' => TxStatus::Idle,
                        b'T' => TxStatus::InTransaction,
                        b'E' => TxStatus::Failed,
                        _ => TxStatus::Idle,
                    }
                };

                if self.phase == ConnPhase::Authenticating {
                    self.phase = ConnPhase::Ready;
                    debug!("Connection authenticated, entering Ready phase");
                }

                ProtoEvent::ConnectionReady { status }
            }

            _ => ProtoEvent::Unknown { tag },
        }
    }
}

impl ProtocolParser for PostgresParser {
    fn try_parse(
        &mut self,
        buf: &[u8],
        direction: Direction,
    ) -> Option<(ProtoEvent, usize)> {
        match self.phase {
            ConnPhase::AwaitingStartup | ConnPhase::AwaitingStartupAfterSslReject => {
                if direction == Direction::Frontend {
                    self.try_parse_startup(buf)
                } else {
                    self.try_parse_regular(buf, direction)
                }
            }
            ConnPhase::Authenticating | ConnPhase::Ready => {
                self.try_parse_regular(buf, direction)
            }
        }
    }

    fn protocol_name(&self) -> &'static str {
        "postgres"
    }

    fn handle_startup_intercept(
        &mut self,
        buf: &[u8],
        direction: Direction,
    ) -> Option<Vec<u8>> {
        if direction != Direction::Frontend {
            return None;
        }

        match self.phase {
            ConnPhase::AwaitingStartup | ConnPhase::AwaitingStartupAfterSslReject => {
                if buf.len() < 8 {
                    return None;
                }

                let version = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

                if version == SSL_REQUEST_CODE {
                    debug!("Intercepting SSLRequest, replying 'N'");
                    self.phase = ConnPhase::AwaitingStartupAfterSslReject;
                    Some(vec![b'N'])
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Extract a null-terminated C string from a byte slice.
fn extract_cstring(buf: &[u8]) -> Option<String> {
    let end = buf.iter().position(|&b| b == 0)?;
    Some(String::from_utf8_lossy(&buf[..end]).into_owned())
}

/// Truncate SQL to MAX_SQL_LEN, respecting UTF-8 char boundaries.
fn truncate_sql(sql: &str) -> String {
    if sql.len() <= MAX_SQL_LEN {
        sql.to_string()
    } else {
        // Find the last char boundary at or before MAX_SQL_LEN
        let mut end = MAX_SQL_LEN;
        while end > 0 && !sql.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &sql[..end])
    }
}

/// Parse row count from a command complete tag.
/// "SELECT 5" -> Some(5), "INSERT 0 3" -> Some(3), "BEGIN" -> None
fn parse_command_tag_rows(tag: &str) -> Option<u64> {
    // The row count is always the last whitespace-separated token
    tag.rsplit_once(' ')
        .and_then(|(_, count)| count.parse().ok())
}

/// Parse ErrorResponse fields into (severity, code, message).
fn parse_error_response(payload: &[u8]) -> (String, String, String) {
    let mut severity = String::new();
    let mut code = String::new();
    let mut message = String::new();

    let mut i = 0;
    while i < payload.len() {
        let field_type = payload[i];
        if field_type == 0 {
            break;
        }
        i += 1;

        let value_start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let value = String::from_utf8_lossy(&payload[value_start..i]).into_owned();
        if i < payload.len() {
            i += 1; // skip null terminator
        }

        match field_type {
            b'S' => severity = value,
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
    }

    (severity, code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_startup_message(version: u32) -> Vec<u8> {
        let length: u32 = 8;
        let mut buf = Vec::new();
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(&version.to_be_bytes());
        buf
    }

    fn make_query_message(sql: &str) -> Vec<u8> {
        let payload_len = sql.len() + 1;
        let length = (payload_len + 4) as u32;
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(sql.as_bytes());
        buf.push(0);
        buf
    }

    fn make_command_complete(tag: &str) -> Vec<u8> {
        let payload_len = tag.len() + 1;
        let length = (payload_len + 4) as u32;
        let mut buf = Vec::new();
        buf.push(b'C');
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(tag.as_bytes());
        buf.push(0);
        buf
    }

    fn make_ready_for_query(status: u8) -> Vec<u8> {
        let length: u32 = 5;
        let mut buf = Vec::new();
        buf.push(b'Z');
        buf.extend_from_slice(&length.to_be_bytes());
        buf.push(status);
        buf
    }

    #[test]
    fn test_ssl_request_intercept() {
        let mut parser = PostgresParser::new();
        let buf = make_startup_message(SSL_REQUEST_CODE);

        let response = parser.handle_startup_intercept(&buf, Direction::Frontend);
        assert_eq!(response, Some(vec![b'N']));
        assert_eq!(parser.phase, ConnPhase::AwaitingStartupAfterSslReject);
    }

    #[test]
    fn test_startup_message() {
        let mut parser = PostgresParser::new();
        let buf = make_startup_message(STARTUP_VERSION_3_0);

        let result = parser.try_parse(&buf, Direction::Frontend);
        assert!(result.is_some());
        assert_eq!(parser.phase, ConnPhase::Authenticating);
    }

    #[test]
    fn test_query_parse() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        let buf = make_query_message("SELECT * FROM users");
        let result = parser.try_parse(&buf, Direction::Frontend);

        match result {
            Some((ProtoEvent::QueryStart { sql }, consumed)) => {
                assert_eq!(sql, "SELECT * FROM users");
                assert_eq!(consumed, buf.len());
            }
            _ => panic!("Expected QueryStart"),
        }
    }

    #[test]
    fn test_command_complete() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        let buf = make_command_complete("SELECT 5");
        let result = parser.try_parse(&buf, Direction::Backend);

        match result {
            Some((ProtoEvent::QueryComplete { tag, rows }, _)) => {
                assert_eq!(tag, "SELECT 5");
                assert_eq!(rows, Some(5));
            }
            _ => panic!("Expected QueryComplete"),
        }
    }

    #[test]
    fn test_ready_for_query() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        let buf = make_ready_for_query(b'I');
        let result = parser.try_parse(&buf, Direction::Backend);

        match result {
            Some((ProtoEvent::ConnectionReady { status }, _)) => {
                assert_eq!(status, TxStatus::Idle);
            }
            _ => panic!("Expected ConnectionReady"),
        }
    }

    #[test]
    fn test_incomplete_message_returns_none() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        let buf = vec![b'Q', 0, 0];
        let result = parser.try_parse(&buf, Direction::Frontend);
        assert!(result.is_none());
    }

    #[test]
    fn test_partial_message_returns_none() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        let buf = make_query_message("SELECT 1");
        let partial = &buf[..buf.len() - 2];
        let result = parser.try_parse(partial, Direction::Frontend);
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_messages_in_buffer() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        let mut buf = make_query_message("SELECT 1");
        buf.extend_from_slice(&make_query_message("SELECT 2"));

        let (event, consumed) = parser.try_parse(&buf, Direction::Frontend).unwrap();
        match event {
            ProtoEvent::QueryStart { sql } => assert_eq!(sql, "SELECT 1"),
            _ => panic!("Expected QueryStart"),
        }

        let (event, _) = parser.try_parse(&buf[consumed..], Direction::Frontend).unwrap();
        match event {
            ProtoEvent::QueryStart { sql } => assert_eq!(sql, "SELECT 2"),
            _ => panic!("Expected QueryStart"),
        }
    }

    #[test]
    fn test_truncate_sql_utf8_boundary() {
        // 4-byte UTF-8 char repeated — truncation must not split a codepoint
        let s = "a".repeat(MAX_SQL_LEN - 1) + "\u{1F600}"; // emoji at the boundary
        let result = truncate_sql(&s);
        assert!(result.ends_with("..."));
        // Must be valid UTF-8 (this would panic if we split mid-codepoint)
        let _ = result.as_bytes();
    }

    #[test]
    fn test_parse_command_tag_insert() {
        assert_eq!(parse_command_tag_rows("INSERT 0 3"), Some(3));
        assert_eq!(parse_command_tag_rows("SELECT 5"), Some(5));
        assert_eq!(parse_command_tag_rows("BEGIN"), None);
        assert_eq!(parse_command_tag_rows("COMMIT"), None);
    }

    // --- Extended Query Protocol helpers ---

    /// Build a Parse message: 'P' + length + stmt_name\0 + sql\0 + 0(i16, no param types)
    fn make_parse_message(stmt_name: &str, sql: &str) -> Vec<u8> {
        let payload_len = stmt_name.len() + 1 + sql.len() + 1 + 2; // name\0 sql\0 paramcount(i16)
        let length = (payload_len + 4) as u32;
        let mut buf = Vec::new();
        buf.push(b'P');
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(stmt_name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(sql.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&0u16.to_be_bytes()); // no param types
        buf
    }

    /// Build a Bind message: 'B' + length + portal\0 + stmt\0 + 0(i16) + 0(i16) + 0(i16)
    fn make_bind_message(portal: &str, stmt_name: &str) -> Vec<u8> {
        // Minimal Bind: portal\0 stmt\0 0(i16 format codes) 0(i16 params) 0(i16 result formats)
        let payload_len = portal.len() + 1 + stmt_name.len() + 1 + 2 + 2 + 2;
        let length = (payload_len + 4) as u32;
        let mut buf = Vec::new();
        buf.push(b'B');
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(portal.as_bytes());
        buf.push(0);
        buf.extend_from_slice(stmt_name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&0u16.to_be_bytes()); // format codes
        buf.extend_from_slice(&0u16.to_be_bytes()); // params
        buf.extend_from_slice(&0u16.to_be_bytes()); // result formats
        buf
    }

    /// Build an Execute message: 'E' + length + portal\0 + max_rows(i32)
    fn make_execute_message(portal: &str) -> Vec<u8> {
        let payload_len = portal.len() + 1 + 4;
        let length = (payload_len + 4) as u32;
        let mut buf = Vec::new();
        buf.push(b'E');
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(portal.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&0u32.to_be_bytes()); // max_rows = 0 (unlimited)
        buf
    }

    /// Build a Close message: 'C' + length + type ('S'/'P') + name\0
    fn make_close_message(close_type: u8, name: &str) -> Vec<u8> {
        let payload_len = 1 + name.len() + 1;
        let length = (payload_len + 4) as u32;
        let mut buf = Vec::new();
        buf.push(b'C');
        buf.extend_from_slice(&length.to_be_bytes());
        buf.push(close_type);
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf
    }

    #[test]
    fn test_extended_bind_execute() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        // Parse
        let parse = make_parse_message("s1", "SELECT * FROM users");
        let (event, _) = parser.try_parse(&parse, Direction::Frontend).unwrap();
        assert!(matches!(event, ProtoEvent::ParseDetected { .. }));

        // Bind
        let bind = make_bind_message("", "s1");
        let (event, _) = parser.try_parse(&bind, Direction::Frontend).unwrap();
        assert!(matches!(event, ProtoEvent::Unknown { .. }));

        // Execute should emit QueryStart with the SQL from Parse
        let exec = make_execute_message("");
        match parser.try_parse(&exec, Direction::Frontend) {
            Some((ProtoEvent::QueryStart { sql }, _)) => {
                assert_eq!(sql, "SELECT * FROM users");
            }
            other => panic!("Expected QueryStart, got {other:?}"),
        }
    }

    #[test]
    fn test_extended_pipeline() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        // Parse a single statement
        let parse = make_parse_message("s1", "INSERT INTO t VALUES ($1)");
        parser.try_parse(&parse, Direction::Frontend).unwrap();

        // Bind + Execute #1 (portal "p1")
        let bind1 = make_bind_message("p1", "s1");
        parser.try_parse(&bind1, Direction::Frontend).unwrap();

        let exec1 = make_execute_message("p1");
        match parser.try_parse(&exec1, Direction::Frontend) {
            Some((ProtoEvent::QueryStart { sql }, _)) => {
                assert_eq!(sql, "INSERT INTO t VALUES ($1)");
            }
            other => panic!("Expected QueryStart #1, got {other:?}"),
        }

        // Bind + Execute #2 (portal "p2", same statement)
        let bind2 = make_bind_message("p2", "s1");
        parser.try_parse(&bind2, Direction::Frontend).unwrap();

        let exec2 = make_execute_message("p2");
        match parser.try_parse(&exec2, Direction::Frontend) {
            Some((ProtoEvent::QueryStart { sql }, _)) => {
                assert_eq!(sql, "INSERT INTO t VALUES ($1)");
            }
            other => panic!("Expected QueryStart #2, got {other:?}"),
        }
    }

    #[test]
    fn test_close_cleans_up() {
        let mut parser = PostgresParser::new();
        parser.phase = ConnPhase::Ready;

        // Parse + Bind
        let parse = make_parse_message("s1", "SELECT 1");
        parser.try_parse(&parse, Direction::Frontend).unwrap();
        let bind = make_bind_message("p1", "s1");
        parser.try_parse(&bind, Direction::Frontend).unwrap();

        assert!(parser.statements.contains_key("s1"));
        assert!(parser.portals.contains_key("p1"));

        // Close portal
        let close_p = make_close_message(b'P', "p1");
        parser.try_parse(&close_p, Direction::Frontend).unwrap();
        assert!(!parser.portals.contains_key("p1"));

        // Close statement
        let close_s = make_close_message(b'S', "s1");
        parser.try_parse(&close_s, Direction::Frontend).unwrap();
        assert!(!parser.statements.contains_key("s1"));
    }
}

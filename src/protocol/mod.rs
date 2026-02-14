pub mod postgres;

use std::fmt;

/// Direction of a message in the proxy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Client -> Server
    Frontend,
    /// Server -> Client
    Backend,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Frontend => write!(f, "->"),
            Direction::Backend => write!(f, "<-"),
        }
    }
}

/// Raw event from the protocol parser — one wire protocol message.
#[derive(Clone, Debug)]
pub enum ProtoEvent {
    QueryStart { sql: String },
    QueryComplete {
        #[allow(dead_code)]
        tag: String,
        rows: Option<u64>,
    },
    QueryError { severity: String, code: String, message: String },
    ConnectionReady { status: TxStatus },
    ParseDetected { sql: String },
    ConnectionClosed,
    Unknown {
        #[allow(dead_code)]
        tag: u8,
    },
}

/// Transaction status from ReadyForQuery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxStatus {
    Idle,
    InTransaction,
    Failed,
}

/// Parses wire protocol for a given database. One instance per connection.
pub trait ProtocolParser: Send + 'static {
    fn try_parse(
        &mut self,
        buf: &[u8],
        direction: Direction,
    ) -> Option<(ProtoEvent, usize)>;

    #[allow(dead_code)]
    fn protocol_name(&self) -> &'static str;

    fn handle_startup_intercept(
        &mut self,
        buf: &[u8],
        direction: Direction,
    ) -> Option<Vec<u8>>;
}

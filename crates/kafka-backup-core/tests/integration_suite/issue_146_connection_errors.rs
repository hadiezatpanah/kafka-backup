//! Issue #146 — connection-error classification drives reconnect-and-retry.
//!
//! `KafkaClient::send_request` reconnects and retries once when a request
//! fails with a connection-level error. Before the fix that decision was made
//! by substring-matching the OS error *message*, which is localized and
//! platform-specific — Windows' `WSAECONNABORTED` / `WSAECONNRESET` texts
//! never matched, so a dropped socket failed the whole run. Classification is
//! now structural (`io::ErrorKind` + raw OS code, see
//! `kafka::connection_error`).
//!
//! These tests exercise the *whole* path with real sockets and real OS
//! errors, in-process and without Docker: a tiny broker mock accepts the
//! client, kills the first connection in a scripted way (TCP RST → `ECONNRESET`
//! / `WSAECONNRESET`, or a clean FIN → `UnexpectedEof`), and answers
//! `Metadata` on the next one. The client must classify, reconnect, retry,
//! and succeed. Because the classification is by kind, the same test is
//! meaningful on Linux, macOS and Windows.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::{ApiKey, BrokerId, MetadataResponse};
use kafka_protocol::protocol::StrBytes;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use kafka_backup_core::config::{KafkaConfig, SecurityConfig, TopicSelection};
use kafka_backup_core::error::KafkaError;
use kafka_backup_core::kafka::{is_connection_error, KafkaClient};

use super::sasl_mock_broker::{read_request, write_response};

/// How the mock kills a connection after reading one request.
#[derive(Clone, Copy, Debug)]
enum Kill {
    /// `SO_LINGER = 0` then drop: the peer sees a TCP RST, i.e.
    /// `ECONNRESET` on Unix / `WSAECONNRESET` (10054) on Windows.
    Reset,
    /// Plain drop: the peer sees FIN, i.e. `UnexpectedEof` ("early eof")
    /// from tokio's `read_exact`.
    Close,
}

struct FlakyBroker {
    addr: std::net::SocketAddr,
    connections: Arc<AtomicUsize>,
    handle: JoinHandle<()>,
}

impl FlakyBroker {
    /// Start a mock that kills the first `kill_first_n` connections after
    /// reading one request, then serves `Metadata` normally on any further
    /// connection.
    async fn start(kill: Kill, kill_first_n: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("local addr");
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = connections.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if n <= kill_first_n {
                    tokio::spawn(kill_after_one_request(stream, kill));
                } else {
                    tokio::spawn(serve_metadata(stream, addr));
                }
            }
        });
        Self {
            addr,
            connections,
            handle,
        }
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

async fn kill_after_one_request(mut stream: TcpStream, kill: Kill) {
    // Read (and ignore) exactly one request so the client is parked in
    // `read_exact` waiting for the response when the socket dies.
    let _ = read_request(&mut stream).await;
    match kill {
        Kill::Reset => {
            let sock = socket2::SockRef::from(&stream);
            sock.set_linger(Some(Duration::ZERO))
                .expect("set SO_LINGER=0 to force RST");
            drop(stream);
        }
        Kill::Close => drop(stream),
    }
}

async fn serve_metadata(mut stream: TcpStream, addr: std::net::SocketAddr) {
    while let Some((api_key, api_version, correlation_id, _body)) = read_request(&mut stream).await
    {
        match api_key {
            ApiKey::Metadata => {
                let resp = MetadataResponse::default()
                    .with_brokers(vec![MetadataResponseBroker::default()
                        .with_node_id(BrokerId(1))
                        .with_host(StrBytes::from_string(addr.ip().to_string()))
                        .with_port(i32::from(addr.port()))])
                    .with_controller_id(BrokerId(1))
                    .with_topics(vec![]);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            other => panic!("mock broker: unexpected request {other:?}"),
        }
    }
}

fn client_for(addr: &std::net::SocketAddr) -> KafkaClient {
    KafkaClient::new(KafkaConfig {
        bootstrap_servers: vec![addr.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
        connection: Default::default(),
    })
}

async fn run_metadata_against(
    kill: Kill,
    kill_first_n: usize,
) -> (kafka_backup_core::Result<usize>, usize) {
    let broker = FlakyBroker::start(kill, kill_first_n).await;
    let client = client_for(&broker.addr);
    client.connect().await.expect("initial TCP connect");

    let result = tokio::time::timeout(Duration::from_secs(20), client.fetch_metadata(None))
        .await
        .expect("fetch_metadata should not hang")
        .map(|topics| topics.len());

    let connections = broker.connections();
    broker.shutdown().await;
    (result, connections)
}

/// TCP RST mid-request → `ConnectionReset` → reconnect → retry succeeds.
#[tokio::test]
async fn test_reconnects_after_tcp_reset() {
    let (result, connections) = run_metadata_against(Kill::Reset, 1).await;
    assert!(
        result.is_ok(),
        "request after a TCP reset should be retried on a fresh connection, got {result:?}"
    );
    assert_eq!(connections, 2, "one killed connection + one reconnect");
}

/// Clean FIN mid-request → `UnexpectedEof` → reconnect → retry succeeds.
#[tokio::test]
async fn test_reconnects_after_peer_close() {
    let (result, connections) = run_metadata_against(Kill::Close, 1).await;
    assert!(
        result.is_ok(),
        "request after a peer close should be retried on a fresh connection, got {result:?}"
    );
    assert_eq!(connections, 2, "one killed connection + one reconnect");
}

/// When the retry also dies, the error that surfaces is the structured
/// `ConnectionIo` carrying the real `io::ErrorKind` — classifiable without
/// reading the (localized) message — and it names the operation.
#[tokio::test]
async fn test_surfaced_error_is_structured_and_classifiable() {
    let (result, connections) = run_metadata_against(Kill::Reset, 2).await;
    let err = result.expect_err("both attempts were reset, so the request must fail");
    assert_eq!(connections, 2, "send_request retries exactly once");
    assert!(
        is_connection_error(&err),
        "surfaced error must classify as a connection error: {err}"
    );
    match &err {
        kafka_backup_core::Error::Kafka(KafkaError::ConnectionIo {
            operation, kind, ..
        }) => {
            assert_eq!(operation, "read response length");
            assert!(
                matches!(
                    kind,
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
                ),
                "unexpected kind {kind:?} (a RST normally surfaces as ConnectionReset; some \
                 stacks deliver EOF first)"
            );
        }
        other => panic!("expected KafkaError::ConnectionIo, got {other:?}"),
    }
}

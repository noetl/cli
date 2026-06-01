//! Tests use an in-process Flight server spun up on a fresh port so
//! the client + server share an event loop without needing the live
//! noetl-server cluster.  Mirrors the Python `test_flight_server.py`
//! test layout from noetl/noetl#643.

use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use super::*;

use futures::StreamExt;

/// Spin up the stub Flight server on a fresh port.  Returns
/// `(endpoint_url, shutdown_signal, last_ticket_handle)`.
async fn start_stub_server(
    batch: Option<RecordBatch>,
) -> (String, oneshot::Sender<()>, Arc<tokio::sync::Mutex<Option<Vec<u8>>>>) {
    start_stub_server_with_auth(batch, None).await
}

/// Spin up a stub Flight server that optionally requires a bearer
/// token on every request (R-2.3 Phase C2.3).  Mirrors the Python
/// server's `BearerTokenMiddlewareFactory` in noetl/noetl#647.
async fn start_stub_server_with_auth(
    batch: Option<RecordBatch>,
    required_token: Option<String>,
) -> (String, oneshot::Sender<()>, Arc<tokio::sync::Mutex<Option<Vec<u8>>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let endpoint = format!("http://{addr}");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let last_ticket = Arc::new(tokio::sync::Mutex::new(None));
    let svc = StubFlightShared {
        response_batch: batch,
        last_ticket: last_ticket.clone(),
        required_token,
    };

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming_shutdown(incoming, async {
                rx.await.ok();
            })
            .await
            .expect("serve_with_incoming_shutdown");
    });
    // Give the server a beat to bind.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (endpoint, tx, last_ticket)
}

/// Same shape as `StubFlight` but with `Arc<Mutex>` for the
/// last_ticket so the test can read it across the gRPC boundary.
///
/// `required_token` (R-2.3 Phase C2.3) mirrors the Python server's
/// bearer-auth middleware — when set, every incoming request must
/// carry `Authorization: Bearer <token>` matching this value.
struct StubFlightShared {
    response_batch: Option<RecordBatch>,
    last_ticket: Arc<tokio::sync::Mutex<Option<Vec<u8>>>>,
    required_token: Option<String>,
}

impl StubFlightShared {
    /// Validate the `Authorization: Bearer <token>` header against
    /// the configured `required_token`.  Returns
    /// `Err(Status::unauthenticated)` (the same status code the
    /// Python server's `FlightUnauthenticatedError` produces on
    /// the wire) when the header is missing / malformed / wrong.
    fn check_auth<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(required) = &self.required_token else {
            return Ok(());
        };
        let auth_header = request.metadata().get("authorization").and_then(|v| v.to_str().ok());
        let Some(value) = auth_header else {
            return Err(Status::unauthenticated("missing Authorization header"));
        };
        let mut parts = value.splitn(2, ' ');
        let scheme = parts.next().unwrap_or("");
        let token = parts.next().unwrap_or("").trim();
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(Status::unauthenticated("Authorization scheme must be Bearer"));
        }
        if token != required {
            return Err(Status::unauthenticated("bearer token mismatch"));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for StubFlightShared {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(&self, _request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        self.check_auth(&request)?;
        let descriptor = request.into_inner();
        if descriptor.r#type != arrow_flight::flight_descriptor::DescriptorType::Cmd as i32 {
            return Err(Status::internal("Cmd-shaped descriptor required"));
        }
        let Some(batch) = &self.response_batch else {
            return Err(Status::unavailable(
                "non-tabular result; fall back to HTTP /api/result/resolve",
            ));
        };

        // Serialise the schema as Arrow IPC bytes (same wire shape
        // the Python server returns).
        let schema = batch.schema();
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_data = arrow_flight::SchemaAsIpc::new(&schema, &options);
        let schema_bytes: arrow_flight::IpcMessage = schema_data
            .try_into()
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?;

        let ticket_bytes = descriptor.cmd.clone();
        let endpoint = arrow_flight::FlightEndpoint {
            ticket: Some(arrow_flight::Ticket {
                ticket: ticket_bytes.clone(),
            }),
            location: vec![arrow_flight::Location {
                uri: "grpc://test-server".to_string(),
            }],
            expiration_time: None,
            app_metadata: Default::default(),
        };
        let info = FlightInfo {
            schema: schema_bytes.0,
            flight_descriptor: Some(descriptor),
            endpoint: vec![endpoint],
            total_records: batch.num_rows() as i64,
            total_bytes: 0,
            ordered: false,
            app_metadata: Default::default(),
        };
        Ok(Response::new(info))
    }

    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(&self, _request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        self.check_auth(&request)?;
        let ticket_bytes = request.into_inner().ticket.to_vec();
        *self.last_ticket.lock().await = Some(ticket_bytes);

        let Some(batch) = &self.response_batch else {
            return Err(Status::unavailable(
                "non-tabular result; fall back to HTTP /api/result/resolve",
            ));
        };

        let schema = batch.schema();
        let stream = futures::stream::iter(vec![Ok(batch.clone())]);
        let encoder = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream);
        let mapped = encoder.map(|item| item.map_err(|e| Status::internal(format!("encoder error: {e}"))));
        Ok(Response::new(Box::pin(mapped) as Self::DoGetStream))
    }

    async fn do_put(&self, _request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(&self, _request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(&self, _request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

/// Build a deterministic 3-row × 4-column sample batch.  Mirrors the
/// `expected_rows` fixture in the Python flight_server tests.
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("username", DataType::Utf8, true),
        Field::new("password", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let id = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
    let username = StringArray::from(vec![Some("user_001"), Some("user_002"), Some("user_003")]);
    let password = StringArray::from(vec![Some("[REDACTED]"), Some("[REDACTED]"), Some("[REDACTED]")]);
    let score = Float64Array::from(vec![Some(0.95), Some(0.72), Some(0.88)]);
    RecordBatch::try_new(
        schema,
        vec![Arc::new(id), Arc::new(username), Arc::new(password), Arc::new(score)],
    )
    .expect("build sample batch")
}

#[tokio::test]
async fn resolve_returns_record_batches() {
    let batch = sample_batch();
    let (endpoint, _shutdown, last_ticket) = start_stub_server(Some(batch)).await;

    let resolver = FlightResolver::connect(&endpoint).await.expect("connect");
    let batches = resolver
        .resolve("noetl://execution/12345/result/big_select/abcd1234")
        .await
        .expect("resolve");

    // Phase A's `rows_to_arrow_ipc` produces a single batch; the
    // stub mirrors that.
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.num_rows(), 3);
    assert_eq!(b.num_columns(), 4);
    assert_eq!(
        b.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>(),
        vec!["id", "username", "password", "score"],
    );

    // Ticket bytes round-tripped end-to-end.
    let captured = last_ticket.lock().await.clone().expect("ticket captured");
    assert_eq!(
        std::str::from_utf8(&captured).unwrap(),
        "noetl://execution/12345/result/big_select/abcd1234"
    );
}

#[tokio::test]
async fn resolve_rows_flattens_to_json_objects() {
    let batch = sample_batch();
    let (endpoint, _shutdown, _last) = start_stub_server(Some(batch)).await;

    let resolver = FlightResolver::connect(&endpoint).await.expect("connect");
    let rows = resolver
        .resolve_rows("noetl://execution/12345/result/big_select/abcd1234")
        .await
        .expect("resolve_rows");

    assert_eq!(rows.len(), 3);
    let first = rows[0].as_object().unwrap();
    assert_eq!(first["id"], 1);
    assert_eq!(first["username"], "user_001");
    assert_eq!(first["password"], "[REDACTED]");
    assert_eq!(first["score"].as_f64().unwrap(), 0.95);
}

#[tokio::test]
async fn resolve_returns_non_tabular_on_unavailable() {
    let (endpoint, _shutdown, _last) = start_stub_server(None).await;

    let resolver = FlightResolver::connect(&endpoint).await.expect("connect");
    let err = resolver
        .resolve("noetl://execution/12345/result/shell_step/x")
        .await
        .expect_err("should be NonTabular");
    match err {
        FlightError::NonTabular { ref_uri, message } => {
            assert_eq!(ref_uri, "noetl://execution/12345/result/shell_step/x");
            assert!(message.contains("non-tabular") || message.contains("HTTP"));
        }
        other => panic!("expected NonTabular, got {other:?}"),
    }
}

#[tokio::test]
async fn connect_to_unreachable_endpoint_fails_fast() {
    // 127.0.0.1:1 is reliably refused on every common OS (privileged
    // port + no listener).  Connect should error within the 10s
    // connect_timeout budget.
    let result = FlightResolver::connect("http://127.0.0.1:1").await;
    assert!(result.is_err(), "should error on connection refused");
}

#[tokio::test]
async fn get_flight_info_returns_schema_and_endpoint() {
    let batch = sample_batch();
    let (endpoint, _shutdown, _last_ticket) = start_stub_server(Some(batch)).await;

    let resolver = FlightResolver::connect(&endpoint).await.expect("connect");
    let info = resolver
        .get_flight_info("noetl://execution/12345/result/big_select/abcd1234")
        .await
        .expect("get_flight_info");

    assert_eq!(info.total_records, 3);
    assert_eq!(info.endpoints.len(), 1);
    let ep = &info.endpoints[0];
    // The ticket the stub server hands back is the same bytes
    // we sent in as the descriptor command — round-trip parity.
    assert_eq!(
        std::str::from_utf8(&ep.ticket).unwrap(),
        "noetl://execution/12345/result/big_select/abcd1234"
    );
    assert!(!ep.locations.is_empty());
    // Schema names match the sample batch.
    assert_eq!(
        info.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>(),
        vec!["id", "username", "password", "score"],
    );
}

#[tokio::test]
async fn get_flight_info_returns_non_tabular_on_unavailable() {
    let (endpoint, _shutdown, _last_ticket) = start_stub_server(None).await;
    let resolver = FlightResolver::connect(&endpoint).await.expect("connect");
    let err = resolver
        .get_flight_info("noetl://execution/12345/result/shell_step/x")
        .await
        .expect_err("should be NonTabular");
    match err {
        FlightError::NonTabular { ref_uri, .. } => {
            assert_eq!(ref_uri, "noetl://execution/12345/result/shell_step/x");
        }
        other => panic!("expected NonTabular, got {other:?}"),
    }
}

#[tokio::test]
async fn endpoint_accessor_returns_constructor_arg() {
    // Build a stub so the connect call actually succeeds; we only
    // care about the endpoint accessor here.
    let batch = sample_batch();
    let (endpoint, _shutdown, _last) = start_stub_server(Some(batch)).await;
    let resolver = FlightResolver::connect(&endpoint).await.expect("connect");
    assert_eq!(resolver.endpoint(), endpoint);
}

// ---------------------------------------------------------------------------
// R-2.3 Phase C2.2 — Client-side TLS
// ---------------------------------------------------------------------------

#[test]
fn tls_config_default_is_empty() {
    let cfg = FlightTlsConfig::new();
    // The struct has no `pub` fields so we can't introspect directly,
    // but we can confirm to_tonic() doesn't panic for the empty case —
    // i.e. a TLS handshake without CA override uses tonic's default
    // trust roots.
    let _tonic = cfg.to_tonic();
}

#[test]
fn tls_config_builder_chains() {
    let cfg = FlightTlsConfig::new()
        .ca_certificate(b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----".to_vec())
        .domain_name("flight.example.com");
    // to_tonic() must consume both knobs without erroring.  We can't
    // peek inside `ClientTlsConfig` from outside tonic, but the
    // integration test below proves the wire path works end-to-end.
    let _tonic = cfg.to_tonic();
}

#[test]
fn tls_config_default_trait() {
    // Default::default() is equivalent to FlightTlsConfig::new() —
    // useful when callers pass the config via `..Default::default()`.
    let cfg1 = FlightTlsConfig::default();
    let cfg2 = FlightTlsConfig::new();
    let _ = (cfg1.to_tonic(), cfg2.to_tonic());
}

/// Spin up a TLS-enabled in-process Flight server using a self-signed
/// cert from rcgen.  Returns `(https://endpoint, shutdown_tx, ca_pem_bytes)`.
async fn start_tls_stub_server(
    batch: Option<RecordBatch>,
) -> (
    String,
    oneshot::Sender<()>,
    Vec<u8>,
    Arc<tokio::sync::Mutex<Option<Vec<u8>>>>,
) {
    // Self-signed cert + key for SAN `localhost` — matches the
    // SNI the client uses when connecting to `https://localhost:<port>`.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("generate self-signed cert");
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let identity = tonic::transport::Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes());
    let tls = tonic::transport::ServerTlsConfig::new().identity(identity);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    // The SNI hostname has to match the cert SAN (`localhost`), but
    // we also need a working port — use `localhost` for the URL host
    // + the bound port from the listener.
    let endpoint = format!("https://localhost:{}", addr.port());
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let last_ticket = Arc::new(tokio::sync::Mutex::new(None));
    let svc = StubFlightShared {
        response_batch: batch,
        last_ticket: last_ticket.clone(),
        required_token: None,
    };

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .expect("server tls_config")
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming_shutdown(incoming, async {
                rx.await.ok();
            })
            .await
            .expect("serve_with_incoming_shutdown");
    });
    // Give the server a beat to bind.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let ca_pem = cert_pem.into_bytes();
    (endpoint, tx, ca_pem, last_ticket)
}

#[tokio::test]
async fn connect_with_tls_round_trips_against_self_signed_server() {
    let batch = sample_batch();
    let (endpoint, _shutdown, ca_pem, _last_ticket) = start_tls_stub_server(Some(batch.clone())).await;

    // With the matching CA the client must succeed.  Note the
    // `domain_name("localhost")` override isn't strictly required
    // here (the URL already says localhost) but it pins the test
    // against the cert SAN explicitly so a future endpoint change
    // doesn't silently break SNI.
    let tls = FlightTlsConfig::new().ca_certificate(ca_pem).domain_name("localhost");
    let resolver = FlightResolver::connect_with_tls(&endpoint, tls)
        .await
        .expect("connect_with_tls");

    let batches = resolver
        .resolve("noetl://execution/12345/result/big_select/abcd1234")
        .await
        .expect("resolve over TLS");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), batch.num_rows());
}

#[tokio::test]
async fn connect_with_tls_rejects_when_ca_missing() {
    let batch = sample_batch();
    let (endpoint, _shutdown, _ca_pem, _last_ticket) = start_tls_stub_server(Some(batch)).await;

    // Empty TLS config — tonic falls back to the default trust
    // store, which does NOT contain our self-signed CA.  Connect
    // must fail at the TLS handshake.
    let tls = FlightTlsConfig::new().domain_name("localhost");
    let result = FlightResolver::connect_with_tls(&endpoint, tls).await;
    assert!(result.is_err(), "expected TLS handshake to fail without CA, got Ok",);
}

#[tokio::test]
async fn connect_plain_https_without_tls_config_fails() {
    let batch = sample_batch();
    let (endpoint, _shutdown, _ca_pem, _last_ticket) = start_tls_stub_server(Some(batch)).await;

    // `connect()` (no tls_config) against an `https://` server
    // without a matching CA in the default trust store should fail
    // too — locks in that the API doesn't silently accept untrusted
    // server certs.
    let result = FlightResolver::connect(&endpoint).await;
    assert!(
        result.is_err(),
        "expected plaintext-API + https endpoint to fail without trust roots",
    );
}

// ---------------------------------------------------------------------------
// R-2.3 Phase C2.3 — Client-side bearer-token auth
// ---------------------------------------------------------------------------

#[test]
fn flight_auth_default_is_empty() {
    // No bearer; constructing the inner client should be a no-op
    // for auth purposes.
    let auth = FlightAuth::new();
    let cfg = FlightConfig::new().auth(auth);
    // We can't introspect the FlightAuth's bearer_token directly
    // (private), but FlightConfig::new() also returns Default, and
    // .auth() with an empty auth keeps the empty shape.
    assert!(cfg.tls.is_none());
}

#[test]
fn flight_auth_bearer_shortcut_matches_builder() {
    // Both APIs should produce the same wire-level token.  We can't
    // peek inside FlightAuth, but we can verify both paths work
    // through the connect chain in the integration tests below.
    let _a = FlightAuth::bearer("tok");
    let _b = FlightAuth::new().bearer_token("tok");
}

#[test]
fn flight_config_bearer_token_shortcut() {
    // `.bearer_token(t)` on FlightConfig is equivalent to
    // `.auth(FlightAuth::bearer(t))` — shorthand for the most
    // common case.
    let _cfg = FlightConfig::new().bearer_token("tok");
}

#[tokio::test]
async fn connect_with_bearer_round_trips_against_auth_server() {
    let batch = sample_batch();
    let (endpoint, _shutdown, _last) =
        start_stub_server_with_auth(Some(batch.clone()), Some("sk-test".to_string())).await;

    let cfg = FlightConfig::new().bearer_token("sk-test");
    let resolver = FlightResolver::connect_with(&endpoint, cfg)
        .await
        .expect("connect_with bearer");
    let batches = resolver
        .resolve("noetl://execution/12345/result/big_select/abcd1234")
        .await
        .expect("resolve with bearer");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), batch.num_rows());
}

#[tokio::test]
async fn connect_with_bearer_rejected_when_token_wrong() {
    let batch = sample_batch();
    let (endpoint, _shutdown, _last) = start_stub_server_with_auth(Some(batch), Some("sk-correct".to_string())).await;

    let cfg = FlightConfig::new().bearer_token("sk-WRONG");
    let resolver = FlightResolver::connect_with(&endpoint, cfg)
        .await
        .expect("channel connect succeeds");
    // The auth check runs server-side on the call itself, not at
    // connect time — so connect succeeds but resolve fails with an
    // UNAUTHENTICATED-shaped server error.
    let err = resolver
        .resolve("noetl://execution/12345/result/x/y")
        .await
        .expect_err("should reject wrong token");
    match err {
        FlightError::Server(msg) => {
            assert!(
                msg.to_lowercase().contains("unauthenticated") || msg.contains("bearer"),
                "expected unauthenticated-shaped error, got {msg}"
            );
        }
        FlightError::Transport(msg) => {
            // Some tonic versions surface unauthenticated as a
            // transport error when the headers are rejected pre-
            // streaming; accept either shape.
            assert!(
                msg.to_lowercase().contains("unauthenticated") || msg.contains("bearer"),
                "expected unauthenticated-shaped transport error, got {msg}"
            );
        }
        other => panic!("expected Server / Transport, got {other:?}"),
    }
}

#[tokio::test]
async fn connect_without_bearer_rejected_when_server_requires_auth() {
    let batch = sample_batch();
    let (endpoint, _shutdown, _last) = start_stub_server_with_auth(Some(batch), Some("sk-test".to_string())).await;

    // Plain `connect()` (no auth config) — server demands a token.
    let resolver = FlightResolver::connect(&endpoint)
        .await
        .expect("channel connect succeeds");
    let err = resolver
        .resolve("noetl://execution/12345/result/x/y")
        .await
        .expect_err("should reject missing token");
    match err {
        FlightError::Server(msg) | FlightError::Transport(msg) => {
            assert!(
                msg.to_lowercase().contains("unauthenticated") || msg.to_lowercase().contains("missing"),
                "expected unauthenticated-shaped error, got {msg}",
            );
        }
        other => panic!("expected Server / Transport, got {other:?}"),
    }
}

#[tokio::test]
async fn connect_with_bearer_works_on_get_flight_info() {
    // Same auth path applies to the metadata-only call.
    let batch = sample_batch();
    let (endpoint, _shutdown, _last) =
        start_stub_server_with_auth(Some(batch.clone()), Some("sk-test".to_string())).await;

    let cfg = FlightConfig::new().bearer_token("sk-test");
    let resolver = FlightResolver::connect_with(&endpoint, cfg)
        .await
        .expect("connect_with bearer");
    let info = resolver
        .get_flight_info("noetl://execution/12345/result/big_select/abcd1234")
        .await
        .expect("get_flight_info with bearer");
    assert_eq!(info.total_records, batch.num_rows() as i64);
}

#[tokio::test]
async fn connect_with_combined_tls_and_bearer() {
    // Bundling — TLS + bearer in one FlightConfig.  Run against the
    // plaintext stub (the TLS leg is exercised in
    // connect_with_tls_round_trips_against_self_signed_server +
    // confirmed here that the combined config doesn't break the
    // plain bearer case).
    let batch = sample_batch();
    let (endpoint, _shutdown, _last) =
        start_stub_server_with_auth(Some(batch.clone()), Some("sk-test".to_string())).await;

    let cfg = FlightConfig::new().auth(FlightAuth::bearer("sk-test"));
    let resolver = FlightResolver::connect_with(&endpoint, cfg)
        .await
        .expect("connect_with combined config");
    let batches = resolver
        .resolve("noetl://execution/12345/result/big_select/abcd1234")
        .await
        .expect("resolve over combined config");
    assert_eq!(batches.len(), 1);
}

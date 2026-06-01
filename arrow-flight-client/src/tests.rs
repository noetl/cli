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
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let endpoint = format!("http://{addr}");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let last_ticket = Arc::new(tokio::sync::Mutex::new(None));
    let svc = StubFlightShared {
        response_batch: batch,
        last_ticket: last_ticket.clone(),
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
struct StubFlightShared {
    response_batch: Option<RecordBatch>,
    last_ticket: Arc<tokio::sync::Mutex<Option<Vec<u8>>>>,
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

    async fn get_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::internal("FlightInfo lookup is not implemented in Phase A"))
    }

    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(&self, _request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
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
async fn endpoint_accessor_returns_constructor_arg() {
    // Build a stub so the connect call actually succeeds; we only
    // care about the endpoint accessor here.
    let batch = sample_batch();
    let (endpoint, _shutdown, _last) = start_stub_server(Some(batch)).await;
    let resolver = FlightResolver::connect(&endpoint).await.expect("connect");
    assert_eq!(resolver.endpoint(), endpoint);
}

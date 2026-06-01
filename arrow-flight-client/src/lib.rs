//! NoETL Arrow Flight client.
//!
//! Mirrors the Python recipe documented on the noetl wiki page
//! [`arrow_flight_result_fetch`][wiki]:
//!
//! ```text
//! ticket = noetl://execution/<eid>/result/<step>/<id>
//!     do_get(ticket) → Arrow IPC stream → RecordBatch
//! ```
//!
//! The server side (R-2.3 Phase A, noetl/noetl#643) treats the
//! ticket bytes as the URI, resolves via `default_store.resolve(ref)`,
//! encodes via `rows_to_arrow_ipc`, and streams `FlightData` back.
//! This crate is the Rust consumer.
//!
//! ## Boundary discipline
//!
//! Per [`agents/rules/execution-model.md`][execution-model] the
//! resolver is a thin RPC client; it does not cache, scrub, or
//! validate the payload — those happen server-side.  The credential
//! scrubbing the server applies to the bytes before encoding
//! round-trips through this client unchanged.
//!
//! ## R-2.3 phase scope
//!
//! Phase B (initial release) ships the standalone client + `resolve`
//! / `resolve_rows` for materialising tabular results into typed
//! `RecordBatch`es / JSON-shaped row dicts.  Phase C1 (this version)
//! adds `get_flight_info` for schema + row-count discovery without
//! materialising the payload — useful for sizing buffers or skipping
//! the fetch entirely for non-tabular refs.
//!
//! Wiring the client into a concrete consumer (the noetl-worker for
//! cross-node tabular reads, the Rust noetl-server once it gains a
//! result-store backend, the CLI tree walker for local-mode
//! consumers) is deferred until a real caller surfaces.  Keeping the
//! client in its own crate avoids coupling those consumers to each
//! other's Cargo build graphs.
//!
//! [wiki]: https://github.com/noetl/noetl/wiki/arrow_flight_result_fetch
//! [execution-model]: https://github.com/noetl/ai-meta/blob/main/agents/rules/execution-model.md

use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, FlightInfo, Ticket};
use futures::TryStreamExt;
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};

/// Typed error variants for `resolve()` failures.  Callers usually
/// match on the variant to decide between fall-back paths (HTTP
/// `/api/result/resolve` for `NonTabular`, a hard error for the
/// rest).
#[derive(Debug, Error)]
pub enum FlightError {
    /// gRPC channel failed to connect or the request itself errored.
    #[error("gRPC transport error: {0}")]
    Transport(String),

    /// Server returned a `FlightUnavailableError` — typically means
    /// the referenced result is non-tabular and the consumer should
    /// fall back to the HTTP `/api/result/resolve` endpoint.
    #[error("server unavailable for ref {ref_uri}: {message}")]
    NonTabular { ref_uri: String, message: String },

    /// Server returned some other typed Flight error.
    #[error("server error: {0}")]
    Server(String),
}

/// FlightInfo discovery summary — R-2.3 Phase C1.
///
/// Returned by [`FlightResolver::get_flight_info`].  Callers use
/// it to size buffers, inspect the schema, or decide whether to
/// follow the embedded endpoints before issuing the actual
/// [`FlightResolver::resolve`] call.
///
/// Field-level mirror of `pyarrow.flight.FlightInfo` (the wire
/// type), with the Arrow schema pre-decoded so consumers don't
/// re-parse the IPC bytes themselves.
#[derive(Debug, Clone)]
pub struct FlightInfoSummary {
    /// Arrow schema of the rowset that `do_get` would stream.
    /// Decoded from the FlightInfo's schema IPC bytes.
    pub schema: SchemaRef,
    /// Total rows the server reports for this ref.  Matches what
    /// `resolve(ref).iter().map(|b| b.num_rows()).sum()` would
    /// produce, without paying for the materialisation.
    pub total_records: i64,
    /// Encoded Arrow IPC byte length.  Matches the byte count
    /// the server would stream over `do_get`.
    pub total_bytes: i64,
    /// gRPC endpoint(s) that can serve the corresponding `do_get`.
    /// Phase C1 returns exactly one; the multi-endpoint variant is
    /// deferred until sharded result tiers land.
    pub endpoints: Vec<FlightEndpointSummary>,
}

/// One endpoint inside a [`FlightInfoSummary`].  Same shape as
/// `pyarrow.flight.FlightEndpoint` but bytes pre-extracted from
/// the wire types.
#[derive(Debug, Clone)]
pub struct FlightEndpointSummary {
    /// Ticket bytes — typically the same `noetl://...` URI the
    /// caller passed to `get_flight_info`.  Consumers with a known
    /// ref URI can skip `get_flight_info` entirely and call
    /// `do_get` directly with the same bytes.
    pub ticket: Vec<u8>,
    /// gRPC URLs that can serve this ticket.  In a single-server
    /// deployment this is `[self.endpoint]`; future multi-endpoint
    /// sharding adds entries here.
    pub locations: Vec<String>,
}

/// Thin wrapper around `arrow_flight::FlightServiceClient` that
/// turns ref URIs into `RecordBatch` streams.
///
/// The resolver owns a long-lived gRPC channel; construct one per
/// (process, server-endpoint) pair and reuse it across `resolve`
/// calls.
#[derive(Clone)]
pub struct FlightResolver {
    client: FlightServiceClient<Channel>,
    endpoint: String,
}

impl FlightResolver {
    /// Connect to the noetl-server's Flight endpoint.  `endpoint`
    /// must be a tonic-compatible URL — `http://...` for plaintext
    /// h2c (default in kind + GKE without TLS) or `https://...` for
    /// TLS-fronted deployments.  Examples:
    /// `http://localhost:8083`, `http://noetl.noetl.svc.cluster.local:8083`,
    /// `https://noetl.example.com:8083`.
    ///
    /// The `grpc://` scheme some Flight clients (Java, Python pyarrow)
    /// accept is NOT valid for tonic — HTTP/2's `:scheme`
    /// pseudo-header must be `http` or `https`, so `grpc://` surfaces
    /// as `Bad :scheme header` at first request time.
    ///
    /// Honors a 10s connect timeout — Flight is on the cluster
    /// network and a slow connect almost always means the server
    /// pod is unhealthy; failing fast lets the caller switch to the
    /// HTTP fallback or surface a clearer error.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint_str = endpoint.into();
        let channel = Endpoint::from_shared(endpoint_str.clone())
            .with_context(|| format!("parse Flight endpoint {endpoint_str}"))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .connect()
            .await
            .with_context(|| format!("connect to Flight endpoint {endpoint_str}"))?;
        let client = FlightServiceClient::new(channel);
        Ok(Self {
            client,
            endpoint: endpoint_str,
        })
    }

    /// Endpoint this resolver is connected to.  Useful for logging.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// R-2.3 Phase C1: fetch the [`FlightInfoSummary`] (schema + row
    /// count + endpoints) for a ref URI without materialising the
    /// underlying rowset.  Useful for clients that want to:
    ///
    /// - Inspect the schema before sizing buffers.
    /// - Decide between Flight + the HTTP fallback based on row
    ///   count / byte total.
    /// - Discover which endpoint to call `do_get` against in a
    ///   future multi-endpoint deployment (Phase C1 always returns
    ///   one endpoint, but the API shape is stable for sharding).
    ///
    /// Wire convention: the descriptor's `cmd` field carries the
    /// noetl:// URI bytes — same convention as the Ticket the
    /// server returns inside the FlightEndpoint, so a consumer
    /// with a known ref URI can skip `get_flight_info` entirely
    /// and call [`resolve`] directly.
    ///
    /// Per `observability.md` Principle 1, the call is wrapped in
    /// a `flight.get_flight_info` span carrying `endpoint` +
    /// `ref_uri`.
    pub async fn get_flight_info(&self, ref_uri: &str) -> Result<FlightInfoSummary, FlightError> {
        let span = tracing::info_span!(
            "flight.get_flight_info",
            endpoint = %self.endpoint,
            ref_uri = %ref_uri,
        );
        let _enter = span.enter();

        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Cmd as i32,
            cmd: ref_uri.as_bytes().to_vec().into(),
            path: Vec::new(),
        };
        let mut client = self.client.clone();
        let info: FlightInfo = match client.get_flight_info(descriptor).await {
            Ok(response) => response.into_inner(),
            Err(status) => return Err(classify_status(ref_uri, &status)),
        };

        // Decode the IPC-encoded schema bytes via arrow-flight's
        // `Schema::try_from(FlightInfo)` impl — clone the info
        // because the impl consumes it, but we still need
        // `info.endpoint` + the totals below.
        let schema: Schema = Schema::try_from(info.clone())
            .map_err(|e| FlightError::Server(format!("decode schema for ref {ref_uri}: {e}")))?;
        let schema_ref: SchemaRef = std::sync::Arc::new(schema);

        let endpoints = info
            .endpoint
            .iter()
            .map(|ep| FlightEndpointSummary {
                ticket: ep.ticket.as_ref().map(|t| t.ticket.to_vec()).unwrap_or_default(),
                locations: ep.location.iter().map(|loc| loc.uri.clone()).collect(),
            })
            .collect();

        tracing::info!(
            endpoint = %self.endpoint,
            ref_uri = %ref_uri,
            total_records = info.total_records,
            total_bytes = info.total_bytes,
            n_endpoints = info.endpoint.len(),
            "flight.get_flight_info completed",
        );

        Ok(FlightInfoSummary {
            schema: schema_ref,
            total_records: info.total_records,
            total_bytes: info.total_bytes,
            endpoints,
        })
    }

    /// Submit `ref_uri` as a Flight Ticket and collect the streamed
    /// RecordBatches.  Each batch carries one chunk of rows; for
    /// small results (Phase A's `rows_to_arrow_ipc` produces a
    /// single batch) the returned Vec has length 1.
    ///
    /// Returns:
    ///
    /// - `Ok(batches)` on success (may be empty if the server
    ///   streamed zero batches).
    /// - `Err(FlightError::NonTabular { .. })` when the server
    ///   returned a `FlightUnavailableError` — typically means the
    ///   ref points at non-tabular data and the consumer should
    ///   fall back to HTTP `/api/result/resolve`.
    /// - `Err(FlightError::Server { .. })` for other server-side
    ///   errors.
    /// - `Err(FlightError::Transport { .. })` for transport-level
    ///   failures (network, gRPC framing, etc.).
    ///
    /// Per `observability.md` Principle 1, the call is wrapped in a
    /// `flight.resolve` span carrying `endpoint` + `ref_uri`.
    pub async fn resolve(&self, ref_uri: &str) -> Result<Vec<RecordBatch>, FlightError> {
        let span = tracing::info_span!(
            "flight.resolve",
            endpoint = %self.endpoint,
            ref_uri = %ref_uri,
        );
        let _enter = span.enter();

        let ticket = Ticket {
            ticket: ref_uri.as_bytes().to_vec().into(),
        };

        let mut client = self.client.clone();
        let stream = match client.do_get(ticket).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                return Err(classify_status(ref_uri, &status));
            }
        };

        // Wrap the raw FlightData stream in the high-level
        // RecordBatchStream decoder; collect into a Vec.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut record_stream =
            FlightRecordBatchStream::new_from_flight_data(stream.map_err(arrow_flight::error::FlightError::Tonic));
        loop {
            match record_stream.try_next().await {
                Ok(Some(batch)) => batches.push(batch),
                Ok(None) => break,
                Err(arrow_flight::error::FlightError::Tonic(status)) => {
                    return Err(classify_status(ref_uri, &status));
                }
                Err(other) => {
                    return Err(FlightError::Server(format!("decode error for ref {ref_uri}: {other}")));
                }
            }
        }
        tracing::info!(
            endpoint = %self.endpoint,
            ref_uri = %ref_uri,
            batches = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "flight.resolve completed",
        );
        Ok(batches)
    }

    /// Convenience: resolve the ref, then flatten the batches into a
    /// `Vec<serde_json::Value>` of row objects.  Mirrors the Python
    /// `client.do_get(ticket).read_all().to_pylist()` recipe.
    ///
    /// Use this when the caller wants JSON-shaped rows; prefer
    /// [`resolve`] when columnar access is enough (`RecordBatch`
    /// avoids the per-row hashmap allocation).
    pub async fn resolve_rows(&self, ref_uri: &str) -> Result<Vec<serde_json::Value>, FlightError> {
        let batches = self.resolve(ref_uri).await?;
        let mut rows: Vec<serde_json::Value> = Vec::new();
        for batch in &batches {
            extend_rows_from_batch(&mut rows, batch)
                .map_err(|e| FlightError::Server(format!("row materialisation error for ref {ref_uri}: {e}")))?;
        }
        Ok(rows)
    }
}

/// Map a tonic `Status` into the typed [`FlightError`] variants.
fn classify_status(ref_uri: &str, status: &tonic::Status) -> FlightError {
    let code = status.code();
    let message = status.message().to_string();
    // Server-side `FlightUnavailableError` lands as gRPC code
    // UNAVAILABLE; treat it as the "non-tabular fall back to HTTP"
    // signal documented by Phase A.
    if code == tonic::Code::Unavailable {
        return FlightError::NonTabular {
            ref_uri: ref_uri.to_string(),
            message,
        };
    }
    if matches!(
        code,
        tonic::Code::Unknown
            | tonic::Code::Internal
            | tonic::Code::FailedPrecondition
            | tonic::Code::Unimplemented
            | tonic::Code::InvalidArgument
            | tonic::Code::NotFound
            | tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
    ) {
        return FlightError::Server(format!("code={:?} message={} ref={}", code, message, ref_uri));
    }
    FlightError::Transport(format!("code={:?} message={} ref={}", code, message, ref_uri))
}

/// Walk a RecordBatch row-by-row, appending JSON-shaped objects to
/// `out`.  Each column converts via Arrow's native -> JSON path.
///
/// `#[allow(clippy::needless_range_loop)]` — we read a typed array
/// at `row_idx` AND write `row_objects[row_idx]` in the same step,
/// so the suggested `iter().enumerate()` over `row_objects` would
/// force a duplicate column traversal.  Index loops are the right
/// shape here.
#[allow(clippy::needless_range_loop)]
fn extend_rows_from_batch(out: &mut Vec<serde_json::Value>, batch: &RecordBatch) -> Result<()> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    let num_rows = batch.num_rows();
    let schema = batch.schema();

    // Pre-allocate empty row objects so we can fill columns in
    // place; one column-pass per field is friendlier to the cache
    // than per-row dispatch through all columns.
    out.reserve(num_rows);
    let mut row_objects: Vec<serde_json::Map<String, serde_json::Value>> =
        (0..num_rows).map(|_| serde_json::Map::new()).collect();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col_name = field.name().clone();
        let column = batch.column(col_idx);

        match column.data_type() {
            DataType::Int64 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("downcast Int64Array")?;
                for row_idx in 0..num_rows {
                    let v: serde_json::Value = if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::from(arr.value(row_idx))
                    };
                    row_objects[row_idx].insert(col_name.clone(), v);
                }
            }
            DataType::Float64 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .context("downcast Float64Array")?;
                for row_idx in 0..num_rows {
                    let v = if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::from(arr.value(row_idx))
                    };
                    row_objects[row_idx].insert(col_name.clone(), v);
                }
            }
            DataType::Boolean => {
                let arr = column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("downcast BooleanArray")?;
                for row_idx in 0..num_rows {
                    let v = if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::Bool(arr.value(row_idx))
                    };
                    row_objects[row_idx].insert(col_name.clone(), v);
                }
            }
            DataType::Utf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("downcast StringArray")?;
                for row_idx in 0..num_rows {
                    let v = if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(arr.value(row_idx).to_string())
                    };
                    row_objects[row_idx].insert(col_name.clone(), v);
                }
            }
            other => {
                // Unsupported column type — fall back to formatted
                // display so callers see SOMETHING rather than a
                // crash.  Future iterations can extend the match.
                for row_idx in 0..num_rows {
                    row_objects[row_idx].insert(
                        col_name.clone(),
                        serde_json::Value::String(format!("<unsupported arrow type {other:?}>")),
                    );
                }
            }
        }
    }

    out.extend(row_objects.into_iter().map(serde_json::Value::Object));
    Ok(())
}

#[cfg(test)]
mod tests;

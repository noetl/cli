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
//! Phase B (this crate) ships the standalone client.  Wiring it into
//! a concrete consumer (the noetl-worker for cross-node tabular
//! reads, the Rust noetl-server once it gains a result-store
//! backend, the CLI tree walker for local-mode consumers) is
//! deferred until a real caller surfaces.  Keeping the client in
//! its own crate avoids coupling those consumers to each other's
//! Cargo build graphs.
//!
//! [wiki]: https://github.com/noetl/noetl/wiki/arrow_flight_result_fetch
//! [execution-model]: https://github.com/noetl/ai-meta/blob/main/agents/rules/execution-model.md

use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
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
    /// must be a gRPC URL such as `grpc://localhost:8083` or
    /// `grpc://noetl.noetl.svc.cluster.local:8083`.
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

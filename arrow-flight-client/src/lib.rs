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
//! Phase B (0.1.0) ships the standalone client + `resolve` /
//! `resolve_rows` for materialising tabular results into typed
//! `RecordBatch`es / JSON-shaped row dicts.  Phase C1 (0.2.0) adds
//! `get_flight_info` for schema + row-count discovery without
//! materialising the payload.  Phase C2.2 (0.3.0) adds
//! [`FlightTlsConfig`] + [`FlightResolver::connect_with_tls`] so the
//! client can talk to a TLS-fronted Flight endpoint (the server side
//! opted into via `NOETL_FLIGHT_TLS_CERT` + `NOETL_FLIGHT_TLS_KEY` in
//! Phase C2.1).  Phase C2.3 (0.4.0) adds [`FlightAuth`] +
//! [`FlightConfig`] + [`FlightResolver::connect_with`] so the
//! client can send `Authorization: Bearer <token>` on every gRPC
//! call, validated by the server's bearer-token middleware
//! (`NOETL_FLIGHT_BEARER_TOKENS`).  Phase C2.4 (0.5.0, this
//! version) adds [`FlightTlsConfig::identity`] so the client can
//! present a client certificate during the TLS handshake when the
//! server requires mutual TLS (`NOETL_FLIGHT_CLIENT_CA`,
//! noetl/noetl#648).
//!
//! See the Phase C2 umbrella at noetl/ai-meta#33 for the wider plan.
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
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

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

/// TLS configuration for the gRPC channel — R-2.3 Phase C2.2.
///
/// Pass to [`FlightResolver::connect_with_tls`] to talk to a
/// TLS-fronted noetl-server Flight endpoint (the `NOETL_FLIGHT_TLS_*`
/// envs the server side opted into in Phase C2.1).  The
/// `ca_certificate` field carries the PEM-encoded server CA bundle —
/// required when the server presents a non-public cert (the typical
/// in-cluster case where a private CA signs the Flight cert).
///
/// `domain_name` overrides the SNI / cert verification hostname.
/// When unset the host portion of the connection URL is used.
/// Useful when the endpoint URL is an IP (or a service-internal DNS
/// name) but the cert was issued for a different SAN.
///
/// Client-side identity (mTLS, Phase C2.4) is configured via
/// `identity(cert_pem, key_pem)`.  When the server is started with
/// `NOETL_FLIGHT_CLIENT_CA` set (noetl/noetl#648) it requires every
/// client to present a cert chaining to that CA on the TLS handshake.
///
/// ## Example — explicit CA + SNI override
///
/// ```no_run
/// # use noetl_arrow_flight_client::{FlightResolver, FlightTlsConfig};
/// # async fn run() -> anyhow::Result<()> {
/// let ca_pem = std::fs::read("/etc/noetl/flight-ca.pem")?;
/// let tls = FlightTlsConfig::new()
///     .ca_certificate(ca_pem)
///     .domain_name("noetl-flight.svc.cluster.local");
///
/// let resolver = FlightResolver::connect_with_tls(
///     "https://noetl.example.com:8083",
///     tls,
/// ).await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Example — mTLS (Phase C2.4)
///
/// ```no_run
/// # use noetl_arrow_flight_client::{FlightResolver, FlightConfig, FlightTlsConfig};
/// # async fn run() -> anyhow::Result<()> {
/// let ca_pem = std::fs::read("/etc/noetl/flight-ca.pem")?;
/// let client_cert = std::fs::read("/etc/noetl/worker-client.crt")?;
/// let client_key = std::fs::read("/etc/noetl/worker-client.key")?;
/// let tls = FlightTlsConfig::new()
///     .ca_certificate(ca_pem)
///     .identity(client_cert, client_key);
/// let cfg = FlightConfig::new().tls(tls);
///
/// let resolver = FlightResolver::connect_with(
///     "https://noetl.example.com:8083",
///     cfg,
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default, Clone)]
pub struct FlightTlsConfig {
    ca_pem: Option<Vec<u8>>,
    domain_name: Option<String>,
    /// R-2.3 Phase C2.4 — `(cert_pem, key_pem)` pair the client
    /// presents on the TLS handshake when the server demands a
    /// client cert.  Both must be set together or both None.
    identity_pem: Option<(Vec<u8>, Vec<u8>)>,
}

impl FlightTlsConfig {
    /// New empty TLS config — relies on the URL scheme + tonic's
    /// built-in defaults (system roots when available).  Equivalent
    /// to passing the URL directly to [`FlightResolver::connect`]
    /// with an `https://` scheme.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the PEM-encoded CA certificate the server's cert must
    /// chain to.  When unset the channel falls back to tonic's
    /// default trust roots (system roots when the `tls-roots`
    /// feature is on; the empty trust store otherwise).
    pub fn ca_certificate(mut self, ca_pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(ca_pem.into());
        self
    }

    /// Override the SNI / cert verification hostname.  Useful when
    /// the endpoint URL is an IP or service-internal DNS but the
    /// cert was issued for a different SAN.
    pub fn domain_name(mut self, name: impl Into<String>) -> Self {
        self.domain_name = Some(name.into());
        self
    }

    /// R-2.3 Phase C2.4: set the client-side identity (cert + key)
    /// the resolver presents on the TLS handshake.  Required when
    /// the noetl-server is started with `NOETL_FLIGHT_CLIENT_CA`
    /// set (mutual TLS).
    ///
    /// Both PEM blobs must be valid — `Identity::from_pem` accepts
    /// them in their on-disk form (the same format the k8s Secret
    /// would mount on the worker pod).  The pair is stored as
    /// `(cert_pem, key_pem)` and converted to a `tonic::transport::Identity`
    /// lazily inside `to_tonic`.
    ///
    /// Per [`agents/rules/execution-model.md`][exec], the client
    /// cert + key are business-logic credentials and should be
    /// resolved from a NoETL-managed secret (k8s Secret mounted on
    /// the worker pod, in the usual case) rather than embedded in
    /// playbook config.
    ///
    /// [exec]: https://github.com/noetl/ai-meta/blob/main/agents/rules/execution-model.md
    pub fn identity(mut self, cert_pem: impl Into<Vec<u8>>, key_pem: impl Into<Vec<u8>>) -> Self {
        self.identity_pem = Some((cert_pem.into(), key_pem.into()));
        self
    }

    /// Compose the underlying `tonic::transport::ClientTlsConfig`.
    fn to_tonic(&self) -> ClientTlsConfig {
        let mut tls = ClientTlsConfig::new();
        if let Some(ca) = &self.ca_pem {
            tls = tls.ca_certificate(Certificate::from_pem(ca));
        }
        if let Some(domain) = &self.domain_name {
            tls = tls.domain_name(domain.clone());
        }
        if let Some((cert_pem, key_pem)) = &self.identity_pem {
            tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
        }
        tls
    }
}

/// Per-call auth configuration — R-2.3 Phase C2.3.
///
/// The token (or future credential variants) is attached to every
/// outgoing gRPC request via tonic metadata, so the server's bearer-
/// token middleware (Phase C2.3 server side, noetl/noetl#647) can
/// validate it.
///
/// Auth is independent of TLS: bearer-on + plaintext is a valid combo
/// for in-cluster deployments behind a separate TLS terminator;
/// bearer-on + TLS is the typical externally-exposed shape.
///
/// Per [`agents/rules/execution-model.md`][exec] the token is a
/// business-logic credential — the caller should resolve it from the
/// NoETL keychain by alias rather than embedding the literal value
/// in playbook config.
///
/// [exec]: https://github.com/noetl/ai-meta/blob/main/agents/rules/execution-model.md
#[derive(Debug, Default, Clone)]
pub struct FlightAuth {
    bearer_token: Option<String>,
}

impl FlightAuth {
    /// New empty auth config (anonymous calls — no `Authorization`
    /// header).
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct directly from a bearer token (convenience for the
    /// common case).  Equivalent to
    /// `FlightAuth::new().bearer_token(token)`.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::new().bearer_token(token)
    }

    /// Set the bearer token sent as `Authorization: Bearer <token>`
    /// on every outgoing gRPC request.  Repeated calls overwrite the
    /// previous value.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }
}

/// Combined connect-time configuration — R-2.3 Phase C2.3.
///
/// Bundles [`FlightTlsConfig`] + [`FlightAuth`] so a caller can
/// describe the full channel shape (TLS trust + bearer token) in one
/// builder, rather than chaining different connect methods.  Both
/// fields are optional and independently controllable.
///
/// ## Example — TLS + bearer
///
/// ```no_run
/// # use noetl_arrow_flight_client::{FlightResolver, FlightConfig, FlightTlsConfig, FlightAuth};
/// # async fn run() -> anyhow::Result<()> {
/// let tls = FlightTlsConfig::new()
///     .ca_certificate(std::fs::read("/etc/noetl/flight-ca.pem")?)
///     .domain_name("noetl-flight.svc.cluster.local");
/// let auth = FlightAuth::bearer("sk-ant-...");
/// let cfg = FlightConfig::new().tls(tls).auth(auth);
///
/// let resolver = FlightResolver::connect_with(
///     "https://noetl.example.com:8083",
///     cfg,
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default, Clone)]
pub struct FlightConfig {
    tls: Option<FlightTlsConfig>,
    auth: Option<FlightAuth>,
}

impl FlightConfig {
    /// New empty config — equivalent to [`FlightResolver::connect`]
    /// without any extras.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a TLS configuration.  See [`FlightTlsConfig`] for the
    /// individual knobs.
    pub fn tls(mut self, tls: FlightTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Attach an auth configuration.  See [`FlightAuth`].
    pub fn auth(mut self, auth: FlightAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Convenience: attach a bearer token without constructing
    /// a [`FlightAuth`] explicitly.  Equivalent to
    /// `cfg.auth(FlightAuth::bearer(token))`.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth = Some(self.auth.unwrap_or_default().bearer_token(token));
        self
    }
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
    bearer_token: Option<String>,
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
    ///
    /// For TLS connections that need an explicit CA bundle (the
    /// typical in-cluster case where a private CA signs the Flight
    /// cert), use [`Self::connect_with_tls`] instead.  Bare
    /// `https://` URLs through this method rely on tonic's default
    /// trust roots, which may not include the cluster CA.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        Self::connect_inner(endpoint.into(), FlightConfig::new()).await
    }

    /// R-2.3 Phase C2.2: connect with explicit TLS configuration.
    ///
    /// Use this when the noetl-server Flight endpoint is TLS-fronted
    /// (Phase C2.1) and the server cert chains to a CA that isn't in
    /// tonic's default trust roots — almost always the case in
    /// cluster-internal deployments where a private CA signs the
    /// Flight cert.
    ///
    /// The endpoint URL must use the `https://` scheme; an `http://`
    /// URL with TLS config attached will fail at the tonic layer.
    /// Bare `https://` without a TLS config relies on the default
    /// trust roots — fine for public TLS, not for private CAs.
    ///
    /// See [`FlightTlsConfig`] for the builder; [Phase C2 umbrella][issue]
    /// for the wider trust-boundary plan.
    ///
    /// For combined TLS + bearer-token connections, prefer
    /// [`Self::connect_with`] with a [`FlightConfig`].
    ///
    /// [issue]: https://github.com/noetl/ai-meta/issues/33
    pub async fn connect_with_tls(endpoint: impl Into<String>, tls: FlightTlsConfig) -> Result<Self> {
        Self::connect_inner(endpoint.into(), FlightConfig::new().tls(tls)).await
    }

    /// R-2.3 Phase C2.3: connect with full channel configuration.
    ///
    /// Accepts a [`FlightConfig`] that bundles optional TLS +
    /// optional bearer-token auth.  Both knobs are independently
    /// opt-in:
    ///
    /// - `FlightConfig::new()` — equivalent to [`Self::connect`].
    /// - `FlightConfig::new().tls(tls)` — equivalent to
    ///   [`Self::connect_with_tls`].
    /// - `FlightConfig::new().bearer_token("…")` — bearer-token only
    ///   (plaintext h2c with bearer is a valid combo when a separate
    ///   TLS terminator fronts the Flight port).
    /// - `FlightConfig::new().tls(tls).bearer_token("…")` —
    ///   TLS + bearer, the typical externally-exposed shape.
    ///
    /// When a bearer token is set, every outgoing gRPC request
    /// includes an `authorization: Bearer <token>` header.  The
    /// server side (noetl/noetl#647) validates the token against
    /// `NOETL_FLIGHT_BEARER_TOKENS` and rejects with
    /// `FlightUnauthenticatedError` on mismatch.
    pub async fn connect_with(endpoint: impl Into<String>, config: FlightConfig) -> Result<Self> {
        Self::connect_inner(endpoint.into(), config).await
    }

    async fn connect_inner(endpoint_str: String, config: FlightConfig) -> Result<Self> {
        let mut endpoint = Endpoint::from_shared(endpoint_str.clone())
            .with_context(|| format!("parse Flight endpoint {endpoint_str}"))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30));

        if let Some(tls) = config.tls {
            endpoint = endpoint
                .tls_config(tls.to_tonic())
                .with_context(|| format!("configure TLS for Flight endpoint {endpoint_str}"))?;
        }

        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("connect to Flight endpoint {endpoint_str}"))?;
        let client = FlightServiceClient::new(channel);
        let bearer_token = config.auth.and_then(|a| a.bearer_token);
        Ok(Self {
            client,
            endpoint: endpoint_str,
            bearer_token,
        })
    }

    /// Attach the configured `Authorization: Bearer <token>` header
    /// to a tonic Request when bearer auth is enabled.  No-op when
    /// the resolver was constructed without auth.
    fn apply_auth<T>(&self, req: &mut tonic::Request<T>) -> Result<(), FlightError> {
        let Some(token) = &self.bearer_token else {
            return Ok(());
        };
        let value_str = format!("Bearer {token}");
        let metadata_value: tonic::metadata::MetadataValue<_> = value_str
            .parse()
            .map_err(|e| FlightError::Transport(format!("invalid bearer token (must be ASCII-safe): {e}")))?;
        req.metadata_mut().insert("authorization", metadata_value);
        Ok(())
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
        let mut req = tonic::Request::new(descriptor);
        self.apply_auth(&mut req)?;
        let mut client = self.client.clone();
        let info: FlightInfo = match client.get_flight_info(req).await {
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
        let mut req = tonic::Request::new(ticket);
        self.apply_auth(&mut req)?;

        let mut client = self.client.clone();
        let stream = match client.do_get(req).await {
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

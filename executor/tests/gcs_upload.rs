//! Integration tests for [`noetl_executor::tools_bridge::gcs_upload_with_store`].
//!
//! Tests here exercise the full `gcs_upload_with_store` call path using an
//! `object_store::memory::InMemory` backend so no real GCS credentials are
//! required in CI.  The unit tests inside `tools_bridge.rs` cover the same
//! happy paths at the module boundary; these integration tests confirm the
//! helper is callable from outside the crate (it is `pub`) and that the
//! `bytes` + `object_store` versions resolve correctly in the workspace.
//!
//! Added in R-3 (noetl/ai-meta#31) alongside the `gcs_upload` helper.

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;

use noetl_executor::tools_bridge::gcs_upload_with_store;

// ---------------------------------------------------------------------------
// Happy path — data round-trips through the store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gcs_upload_roundtrip_json_payload() {
    let store = Arc::new(InMemory::new());
    let payload = r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#;

    gcs_upload_with_store(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        "exports/users.json",
        payload,
    )
    .await
    .expect("upload should succeed on InMemory store");

    let path = StorePath::from("exports/users.json");
    let got = store
        .get(&path)
        .await
        .expect("should find the uploaded object")
        .bytes()
        .await
        .expect("should read bytes");

    assert_eq!(got.as_ref(), payload.as_bytes());
}

#[tokio::test]
async fn gcs_upload_roundtrip_csv_payload() {
    let store = Arc::new(InMemory::new());
    let payload = "id,name\n1,alice\n2,bob\n";

    gcs_upload_with_store(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        "exports/users.csv",
        payload,
    )
    .await
    .unwrap();

    let path = StorePath::from("exports/users.csv");
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), payload.as_bytes());
}

#[tokio::test]
async fn gcs_upload_overwrites_on_repeated_put() {
    // Verifies that calling gcs_upload_with_store twice on the same key
    // replaces the content — matches the GCS object-level PUT contract.
    let store = Arc::new(InMemory::new());
    let key = "reports/daily.json";

    gcs_upload_with_store(Arc::clone(&store) as Arc<dyn ObjectStore>, key, "first").await.unwrap();
    gcs_upload_with_store(Arc::clone(&store) as Arc<dyn ObjectStore>, key, "second").await.unwrap();

    let path = StorePath::from(key);
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"second");
}

#[tokio::test]
async fn gcs_upload_empty_string_succeeds() {
    // An empty payload is a valid GCS object and must not error.
    let store = Arc::new(InMemory::new());

    gcs_upload_with_store(Arc::clone(&store) as Arc<dyn ObjectStore>, "empty.json", "").await.unwrap();

    let path = StorePath::from("empty.json");
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), 0);
}

#[tokio::test]
async fn gcs_upload_nested_key_preserves_full_path() {
    // GCS keys that contain slashes must be stored and retrieved using
    // the full slash-separated key string.
    let store = Arc::new(InMemory::new());
    let key = "year=2026/month=06/day=01/run-42/output.json";

    gcs_upload_with_store(Arc::clone(&store) as Arc<dyn ObjectStore>, key, "{}").await.unwrap();

    let path = StorePath::from(key);
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"{}");
}

// ---------------------------------------------------------------------------
// Auth config shape test — confirms gcs_upload (the production wrapper)
// accepts the same (bucket, key, data) signature the CLI's sink_to_gcs
// replacement calls with, and returns anyhow::Result<()>.  No real GCS
// call is made — the test just verifies the function signature compiles
// and returns the right type.  Actual network paths are covered by the
// InMemory tests above.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn _assert_gcs_upload_signature() {
    // This function is never called; it exists to make the compiler verify
    // that `gcs_upload` has the expected signature so a future refactor
    // that changes it fails this test at compile time.
    async fn _inner() -> anyhow::Result<()> {
        noetl_executor::tools_bridge::gcs_upload("my-bucket", "path/to/file.json", "data").await
    }
}

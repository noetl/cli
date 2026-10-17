//! Local credential store for the CLI local runtime.
//!
//! Local mode has no server-side keychain, so a playbook's credential alias
//! resolves here, in order:
//!
//! 1. the environment variable [`env_var`]`(alias)` —
//!    `NOETL_CREDENTIAL_<ALIAS>`, when set to a non-blank value;
//! 2. the file `<credentials dir>/<alias>` ([`credentials_dir`]), which must
//!    not be readable by group or others (mode `600`).
//!
//! The playbook always names the alias; neither source is an ambient default.
//! Files are written by `noetl credential set|google`, which create them with
//! the right modes.
//!
//! A `google_oauth` credential is a JSON document holding a refresh token;
//! [`google_access_token`] exchanges it for a short-lived access token, cached
//! in-process until shortly before it expires, so the refresh token never
//! enters playbook variables or step results.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Default Google OAuth token endpoint.
pub const GOOGLE_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";

/// Environment variable backing `alias`: `NOETL_CREDENTIAL_` + the alias
/// upper-cased, every non-alphanumeric character replaced by `_`
/// (`gmail-work` → `NOETL_CREDENTIAL_GMAIL_WORK`).
pub fn env_var(alias: &str) -> String {
    let suffix: String = alias
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' })
        .collect();
    format!("NOETL_CREDENTIAL_{}", suffix)
}

/// Directory holding credential files: `$NOETL_CREDENTIALS_DIR`, else
/// `$XDG_CONFIG_HOME/noetl/credentials`, else `~/.config/noetl/credentials`.
pub fn credentials_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("NOETL_CREDENTIALS_DIR").filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(dir));
    }
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(config.join("noetl").join("credentials"))
}

/// An alias may only contain ASCII letters, digits, `-`, `_` and `.`, and may
/// not start with `.`, so it can never name a path outside the store.
pub fn validate_alias(alias: &str) -> Result<()> {
    let ok = !alias.is_empty()
        && !alias.starts_with('.')
        && alias
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if ok {
        Ok(())
    } else {
        anyhow::bail!(
            "invalid credential alias `{}`: use letters, digits, `-`, `_`, `.` (not leading)",
            alias
        )
    }
}

/// Resolve `alias` to its secret value (trimmed).  See the module docs for
/// the lookup order.  An alias found nowhere is an error naming both places.
pub fn resolve(alias: &str) -> Result<String> {
    validate_alias(alias)?;
    let var = env_var(alias);
    if let Some(v) = std::env::var(&var).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        return Ok(v);
    }
    let path = credentials_dir().map(|d| d.join(alias));
    if let Some(path) = path.as_deref().filter(|p| p.exists()) {
        check_private(path)?;
        let value = std::fs::read_to_string(path)
            .with_context(|| format!("reading credential file {}", path.display()))?;
        let value = value.trim().to_string();
        if value.is_empty() {
            anyhow::bail!("credential file {} is empty", path.display());
        }
        return Ok(value);
    }
    anyhow::bail!(
        "credential alias `{}` is not set or empty; local mode reads ${} or the file {}",
        alias,
        var,
        path.map(|p| p.display().to_string())
            .unwrap_or_else(|| "<no credentials dir: HOME unset>".to_string())
    )
}

/// Refuse a credential file that group or others can read.
#[cfg(unix)]
fn check_private(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)
        .with_context(|| format!("reading {}", path.display()))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        anyhow::bail!(
            "credential file {} is readable by others (mode {:o}); run: chmod 600 {}",
            path.display(),
            mode & 0o777,
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private(_path: &Path) -> Result<()> {
    Ok(())
}

/// Write `value` as credential `alias` (directory `700`, file `600`),
/// replacing any existing one.  Returns the file path.
pub fn store(alias: &str, value: &str) -> Result<PathBuf> {
    validate_alias(alias)?;
    let dir = credentials_dir().context("cannot locate the credentials dir (HOME unset)")?;
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let path = dir.join(alias);
    let tmp = dir.join(format!(".{}.tmp", alias));
    {
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp).with_context(|| format!("writing {}", tmp.display()))?;
        file.write_all(value.trim().as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &path).with_context(|| format!("replacing {}", path.display()))?;
    Ok(path)
}

/// One stored credential, without its value.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredCredential {
    pub alias: String,
    /// `google_oauth` for a Google refresh-token credential, else `secret`.
    pub kind: String,
    pub mode: u32,
}

/// Credentials in the store, sorted by alias.  Values are never returned.
pub fn list() -> Result<Vec<StoredCredential>> {
    let Some(dir) = credentials_dir().filter(|d| d.is_dir()) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let alias = entry.file_name().to_string_lossy().to_string();
        if validate_alias(&alias).is_err() || !entry.file_type()?.is_file() {
            continue;
        }
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            entry.metadata()?.permissions().mode() & 0o777
        };
        #[cfg(not(unix))]
        let mode = 0;
        let kind = std::fs::read_to_string(entry.path())
            .ok()
            .and_then(|v| serde_json::from_str::<GoogleOauthCredential>(v.trim()).ok())
            .filter(|c| c.kind == "google_oauth")
            .map(|_| "google_oauth".to_string())
            .unwrap_or_else(|| "secret".to_string());
        out.push(StoredCredential { alias, kind, mode });
    }
    out.sort_by(|a, b| a.alias.cmp(&b.alias));
    Ok(out)
}

/// A Google OAuth refresh-token credential, as `noetl credential google`
/// writes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleOauthCredential {
    #[serde(rename = "type")]
    pub kind: String,
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    /// The account the token belongs to, when known — for `list` / humans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
}

fn default_token_uri() -> String {
    GOOGLE_TOKEN_URI.to_string()
}

/// Whether `value` is a `google_oauth` credential document.
pub fn is_google_oauth(value: &str) -> bool {
    serde_json::from_str::<GoogleOauthCredential>(value)
        .map(|c| c.kind == "google_oauth")
        .unwrap_or(false)
}

fn token_cache() -> &'static Mutex<HashMap<String, (String, Instant)>> {
    static CACHE: OnceLock<Mutex<HashMap<String, (String, Instant)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Exchange the `google_oauth` credential `alias` for an access token.
///
/// Tokens are cached per alias until 60 s before they expire, so an agent
/// calling a Gmail tool many times in one run refreshes once.
pub async fn google_access_token(alias: &str) -> Result<String> {
    if let Some((token, until)) = token_cache().lock().unwrap().get(alias).cloned() {
        if Instant::now() < until {
            return Ok(token);
        }
    }
    let raw = resolve(alias)?;
    let cred: GoogleOauthCredential = serde_json::from_str(&raw)
        .ok()
        .filter(|c: &GoogleOauthCredential| c.kind == "google_oauth")
        .with_context(|| {
            format!(
                "credential `{}` is not a google_oauth credential; create it with: noetl credential google {} --client-secret-file <file>",
                alias, alias
            )
        })?;

    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?
        .post(&cred.token_uri)
        .form(&[
            ("grant_type", "refresh_token"),
            ("client_id", cred.client_id.as_str()),
            ("client_secret", cred.client_secret.as_str()),
            ("refresh_token", cred.refresh_token.as_str()),
        ])
        .send()
        .await
        .with_context(|| format!("refreshing google_oauth credential `{}`", alias))?;
    let status = response.status();
    let body: serde_json::Value = response.json().await.unwrap_or_default();
    if !status.is_success() {
        let error = body["error"].as_str().unwrap_or("unknown_error");
        let hint = if error == "invalid_grant" {
            format!(
                " — the refresh token was revoked or expired; re-authorize with: noetl credential google {} --client-secret-file <file>",
                alias
            )
        } else {
            String::new()
        };
        anyhow::bail!(
            "google token refresh for `{}` failed with {}: {}{}",
            alias,
            status.as_u16(),
            error,
            hint
        );
    }
    let token = body["access_token"]
        .as_str()
        .filter(|t| !t.is_empty())
        .context("google token response has no access_token")?
        .to_string();
    let ttl = body["expires_in"].as_u64().unwrap_or(3600).saturating_sub(60);
    token_cache()
        .lock()
        .unwrap()
        .insert(alias.to_string(), (token.clone(), Instant::now() + Duration::from_secs(ttl)));
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each test gets its own store directory through NOETL_CREDENTIALS_DIR.
    /// Tests that touch it run serially via this lock (env is process-wide).
    fn with_store<T>(name: &str, f: impl FnOnce(&Path) -> T) -> T {
        static LOCK: Mutex<()> = Mutex::new(());
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("noetl-creds-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("NOETL_CREDENTIALS_DIR", &dir);
        let out = f(&dir);
        std::env::remove_var("NOETL_CREDENTIALS_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        out
    }

    #[test]
    fn env_var_normalises_alias() {
        assert_eq!(env_var("gmail-work.2"), "NOETL_CREDENTIAL_GMAIL_WORK_2");
    }

    #[test]
    fn aliases_cannot_escape_the_store() {
        for bad in ["", "../x", ".hidden", "a/b", "a b", "x\0"] {
            assert!(validate_alias(bad).is_err(), "{bad:?} accepted");
        }
        assert!(validate_alias("gmail-work").is_ok());
    }

    #[test]
    fn store_then_resolve_round_trips_with_private_modes() {
        with_store("roundtrip", |dir| {
            let path = store("lc-test-rt", "  s3cret \n").unwrap();
            assert_eq!(resolve("lc-test-rt").unwrap(), "s3cret");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
                assert_eq!(std::fs::metadata(dir).unwrap().permissions().mode() & 0o777, 0o700);
            }
            let listed = list().unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].alias, "lc-test-rt");
            assert_eq!(listed[0].kind, "secret");
        });
    }

    #[test]
    fn env_var_wins_over_file() {
        with_store("envwins", |_| {
            store("lc-test-env", "from-file").unwrap();
            std::env::set_var("NOETL_CREDENTIAL_LC_TEST_ENV", "from-env");
            let got = resolve("lc-test-env").unwrap();
            std::env::remove_var("NOETL_CREDENTIAL_LC_TEST_ENV");
            assert_eq!(got, "from-env");
        });
    }

    #[cfg(unix)]
    #[test]
    fn world_readable_file_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        with_store("worldread", |_| {
            let path = store("lc-test-open", "x").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            let err = resolve("lc-test-open").unwrap_err().to_string();
            assert!(err.contains("chmod 600"), "{err}");
        });
    }

    #[test]
    fn missing_alias_names_both_sources() {
        with_store("missing", |_| {
            let err = resolve("lc-test-missing").unwrap_err().to_string();
            assert!(err.contains("NOETL_CREDENTIAL_LC_TEST_MISSING"), "{err}");
            assert!(err.contains("lc-test-missing"), "{err}");
        });
    }

    #[tokio::test]
    async fn google_refresh_exchanges_and_caches() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("refresh_token=rt-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "at-1", "expires_in": 3599, "token_type": "Bearer"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let doc = serde_json::json!({
            "type": "google_oauth", "client_id": "cid", "client_secret": "cs",
            "refresh_token": "rt-1", "token_uri": format!("{}/token", server.uri()),
        })
        .to_string();
        assert!(is_google_oauth(&doc));
        std::env::set_var("NOETL_CREDENTIAL_LC_TEST_GOOGLE", &doc);
        let first = google_access_token("lc-test-google").await.unwrap();
        let second = google_access_token("lc-test-google").await.unwrap();
        std::env::remove_var("NOETL_CREDENTIAL_LC_TEST_GOOGLE");
        assert_eq!(first, "at-1");
        assert_eq!(second, "at-1", "second call must come from the cache");
    }

    #[tokio::test]
    async fn google_invalid_grant_says_how_to_fix() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant", "error_description": "Token has been expired or revoked."
            })))
            .mount(&server)
            .await;
        let doc = serde_json::json!({
            "type": "google_oauth", "client_id": "cid", "client_secret": "cs",
            "refresh_token": "rt-bad", "token_uri": server.uri(),
        })
        .to_string();
        std::env::set_var("NOETL_CREDENTIAL_LC_TEST_GOOGLE_BAD", &doc);
        let err = google_access_token("lc-test-google-bad").await.unwrap_err().to_string();
        std::env::remove_var("NOETL_CREDENTIAL_LC_TEST_GOOGLE_BAD");
        assert!(err.contains("invalid_grant") && err.contains("noetl credential google"), "{err}");
        assert!(!err.contains("rt-bad") && !err.contains("cs\""), "secrets leaked: {err}");
    }

    #[tokio::test]
    async fn plain_secret_is_not_a_google_credential() {
        std::env::set_var("NOETL_CREDENTIAL_LC_TEST_PLAIN", "sk-plain");
        let err = google_access_token("lc-test-plain").await.unwrap_err().to_string();
        std::env::remove_var("NOETL_CREDENTIAL_LC_TEST_PLAIN");
        assert!(err.contains("not a google_oauth credential"), "{err}");
    }
}

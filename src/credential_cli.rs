//! `noetl credential` — manage the local credential store that `--runtime
//! local` playbooks resolve `credential:` aliases from
//! (`noetl_executor::local_credentials`).
//!
//! Values are read from a hidden prompt or stdin and written to `600` files;
//! no subcommand prints a stored value.

use anyhow::{Context, Result};
use clap::Subcommand;
use noetl_executor::local_credentials::{self, GoogleOauthCredential, GOOGLE_TOKEN_URI};
use std::io::Read;
use tokio::net::TcpListener;
use url::Url;

const GOOGLE_AUTH_URI: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const GMAIL_PROFILE_URL: &str = "https://gmail.googleapis.com/gmail/v1/users/me/profile";

#[derive(Subcommand, Debug)]
pub(crate) enum CredentialCommand {
    /// Store a secret (API key, token) under an alias.  Reads a hidden prompt,
    /// or stdin when piped.
    /// Examples:
    ///     noetl credential set anthropic
    ///     noetl credential set anthropic < key.txt
    #[command(verbatim_doc_comment)]
    Set {
        /// Alias playbooks reference with `credential: <alias>`
        alias: String,
    },
    /// Authorize a Google account in the browser and store its refresh token
    /// as a `google_oauth` credential (default scope: gmail.readonly).
    /// Examples:
    ///     noetl credential google gmail-work --client-secret-file client_secret.json
    ///     noetl credential google gmail-home --client-secret-file client_secret.json --login-hint me@gmail.com
    #[command(verbatim_doc_comment)]
    Google {
        /// Alias playbooks reference with `credential: <alias>`
        alias: String,
        /// OAuth client JSON downloaded from Google Cloud Console (Desktop app)
        #[arg(long)]
        client_secret_file: std::path::PathBuf,
        /// OAuth scope; repeatable.  Short names expand to
        /// https://www.googleapis.com/auth/<name>
        #[arg(long = "scope", default_value = "gmail.readonly")]
        scopes: Vec<String>,
        /// Pre-select this Google account on the consent screen
        #[arg(long)]
        login_hint: Option<String>,
        /// Loopback port for the redirect (0 = pick a free one)
        #[arg(long, default_value_t = 0)]
        port: u16,
        /// Print the URL instead of opening a browser
        #[arg(long)]
        no_open: bool,
    },
    /// List stored aliases with their type and file mode (never values)
    List,
    /// Print the credentials directory
    Path,
}

pub(crate) async fn handle_credential_command(command: CredentialCommand) -> Result<()> {
    match command {
        CredentialCommand::Set { alias } => set(&alias),
        CredentialCommand::Google {
            alias,
            client_secret_file,
            scopes,
            login_hint,
            port,
            no_open,
        } => google(&alias, &client_secret_file, &scopes, login_hint.as_deref(), port, no_open).await,
        CredentialCommand::List => list(),
        CredentialCommand::Path => {
            let dir = local_credentials::credentials_dir().context("cannot locate the credentials dir (HOME unset)")?;
            println!("{}", dir.display());
            Ok(())
        }
    }
}

fn set(alias: &str) -> Result<()> {
    local_credentials::validate_alias(alias)?;
    let value = if atty_stdin() {
        rpassword::prompt_password(format!("Value for `{}` (hidden): ", alias))?
    } else {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        buf
    };
    if value.trim().is_empty() {
        anyhow::bail!("empty value; nothing stored");
    }
    let path = local_credentials::store(alias, &value)?;
    println!("Stored `{}` at {} (mode 600)", alias, path.display());
    Ok(())
}

fn atty_stdin() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal()
}

fn list() -> Result<()> {
    let creds = local_credentials::list()?;
    if creds.is_empty() {
        println!("No stored credentials.");
        return Ok(());
    }
    println!("{:<32} {:<14} MODE", "ALIAS", "TYPE");
    for c in creds {
        let warn = if c.mode & 0o077 != 0 { "  ⚠ not private: chmod 600" } else { "" };
        println!("{:<32} {:<14} {:o}{}", c.alias, c.kind, c.mode, warn);
    }
    Ok(())
}

/// The fields of a Google OAuth client JSON (`installed` or `web` section).
#[derive(Debug)]
struct GoogleClient {
    client_id: String,
    client_secret: String,
    auth_uri: String,
    token_uri: String,
}

fn read_google_client(path: &std::path::Path) -> Result<GoogleClient> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let doc: serde_json::Value =
        serde_json::from_str(&raw).with_context(|| format!("{} is not JSON", path.display()))?;
    let section = doc
        .get("installed")
        .or_else(|| doc.get("web"))
        .context("client secret file has no `installed` section; create a Desktop app OAuth client")?;
    let field = |name: &str| -> Result<String> {
        section[name]
            .as_str()
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .with_context(|| format!("client secret file is missing `{}`", name))
    };
    Ok(GoogleClient {
        client_id: field("client_id")?,
        client_secret: field("client_secret")?,
        auth_uri: section["auth_uri"].as_str().unwrap_or(GOOGLE_AUTH_URI).to_string(),
        token_uri: section["token_uri"].as_str().unwrap_or(GOOGLE_TOKEN_URI).to_string(),
    })
}

fn expand_scope(scope: &str) -> String {
    if scope.starts_with("https://") || scope.contains(':') {
        scope.to_string()
    } else {
        format!("https://www.googleapis.com/auth/{}", scope)
    }
}

async fn google(
    alias: &str,
    client_secret_file: &std::path::Path,
    scopes: &[String],
    login_hint: Option<&str>,
    port: u16,
    no_open: bool,
) -> Result<()> {
    local_credentials::validate_alias(alias)?;
    let client = read_google_client(client_secret_file)?;
    let scopes: Vec<String> = scopes.iter().map(|s| expand_scope(s)).collect();

    let listener = TcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("binding 127.0.0.1:{}", port))?;
    let redirect_uri = format!("http://127.0.0.1:{}/", listener.local_addr()?.port());

    let verifier = super::build_pkce_code_verifier();
    let challenge = super::build_pkce_code_challenge(&verifier);
    let state = super::generate_random_urlsafe(24);

    let mut url = Url::parse(&client.auth_uri).context("invalid auth_uri in client secret file")?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("client_id", &client.client_id)
            .append_pair("redirect_uri", &redirect_uri)
            .append_pair("response_type", "code")
            .append_pair("scope", &scopes.join(" "))
            .append_pair("access_type", "offline")
            .append_pair("prompt", "consent")
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", &state);
        if let Some(hint) = login_hint {
            q.append_pair("login_hint", hint);
        }
    }

    eprintln!("Authorize `{}` in your browser:\n\n  {}\n", alias, url);
    if !no_open && super::open_browser(url.as_str()).is_err() {
        eprintln!("(could not open a browser; open the URL above manually)");
    }
    eprintln!("Waiting for Google to redirect to {} …", redirect_uri);

    let code = super::wait_for_pkce_callback(&listener, "/", &state).await?;

    let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(30)).build()?;
    let response = http
        .post(&client.token_uri)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("client_id", client.client_id.as_str()),
            ("client_secret", client.client_secret.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
            ("code_verifier", verifier.as_str()),
        ])
        .send()
        .await
        .context("exchanging the authorization code")?;
    let status = response.status();
    let body: serde_json::Value = response.json().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "Google token exchange failed with {}: {} {}",
            status.as_u16(),
            body["error"].as_str().unwrap_or("unknown_error"),
            body["error_description"].as_str().unwrap_or("")
        );
    }
    let refresh_token = body["refresh_token"].as_str().filter(|t| !t.is_empty()).context(
        "Google returned no refresh_token. Remove this app's access at https://myaccount.google.com/permissions and run the command again",
    )?;

    // Name the account the token belongs to, when the scope allows it.
    let mut account = None;
    if let Some(access_token) = body["access_token"].as_str() {
        if scopes.iter().any(|s| s.contains("/auth/gmail.")) {
            if let Ok(resp) = http.get(GMAIL_PROFILE_URL).bearer_auth(access_token).send().await {
                if let Ok(profile) = resp.json::<serde_json::Value>().await {
                    account = profile["emailAddress"].as_str().map(str::to_string);
                }
            }
        }
    }

    let credential = GoogleOauthCredential {
        kind: "google_oauth".to_string(),
        client_id: client.client_id,
        client_secret: client.client_secret,
        refresh_token: refresh_token.to_string(),
        token_uri: client.token_uri,
        scopes: scopes.clone(),
        account: account.clone(),
    };
    let path = local_credentials::store(alias, &serde_json::to_string(&credential)?)?;
    println!(
        "Stored google_oauth credential `{}`{} at {} (mode 600); scopes: {}",
        alias,
        account.map(|a| format!(" for {}", a)).unwrap_or_default(),
        path.display(),
        scopes.join(" ")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_short_names_expand() {
        assert_eq!(expand_scope("gmail.readonly"), "https://www.googleapis.com/auth/gmail.readonly");
        assert_eq!(
            expand_scope("https://www.googleapis.com/auth/gmail.modify"),
            "https://www.googleapis.com/auth/gmail.modify"
        );
    }

    #[test]
    fn reads_desktop_client_secret_file() {
        let path = std::env::temp_dir().join(format!("noetl-client-secret-{}.json", std::process::id()));
        std::fs::write(
            &path,
            r#"{"installed":{"client_id":"cid.apps.googleusercontent.com","client_secret":"cs","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","redirect_uris":["http://localhost"]}}"#,
        )
        .unwrap();
        let client = read_google_client(&path).unwrap();
        std::fs::remove_file(&path).ok();
        assert_eq!(client.client_id, "cid.apps.googleusercontent.com");
        assert_eq!(client.token_uri, "https://oauth2.googleapis.com/token");
    }

    #[test]
    fn rejects_a_client_file_without_installed_section() {
        let path = std::env::temp_dir().join(format!("noetl-client-secret-bad-{}.json", std::process::id()));
        std::fs::write(&path, r#"{"type":"service_account"}"#).unwrap();
        let err = read_google_client(&path).unwrap_err().to_string();
        std::fs::remove_file(&path).ok();
        assert!(err.contains("Desktop app"), "{err}");
    }
}

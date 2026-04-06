# NoETL CLI Project Instructions (Gemini CLI)

## Automation Commands

**Always use the `noetl` binary** (or `ntl` alias) for running automation tasks.

### Common Commands

```bash
# Execute a playbook locally
noetl run <playbook_path> [--set key=value]

# Pass variables
noetl run playbook.yaml --set env=prod --set version=v2.5.5

# Server management
noetl server start [--init-db]
noetl server stop [--force]

# Worker management
noetl worker start [--max-workers 4]
noetl worker stop [--name <name>] [--force]

# Database operations
noetl db init
noetl db validate

# Build Docker images
noetl build [--no-cache]

# Kubernetes deployment
noetl k8s deploy
noetl k8s redeploy [--no-cache]
noetl k8s reset [--no-cache]
noetl k8s remove

# Context management
noetl context add <name> --server-url <url> [--set-current]
noetl context list
noetl context use <name>
noetl context current

# Auth management
noetl auth login --browser-pkce
noetl auth logout

# Gateway mode
noetl --gateway catalog list Playbook
noetl --gateway exec <playbook_id>

# SQL queries
noetl query "SELECT * FROM noetl.keychain LIMIT 5"
```

### Installation

```bash
# Via Cargo
cargo install --bins noetl

# Via Homebrew
brew tap noetl/tap
brew install noetl
```

## Project Structure

- `src/` - Rust source code
- `Cargo.toml` - Crate configuration and dependencies
- `Dockerfile` - Container build definition
- `README.md` - Usage documentation and release channels
- `.github/` - Release workflows and CI

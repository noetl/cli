//! Where `noetl serve` keeps agent tasks.
//!
//! - [`TaskStore::Memory`] (the default) is the original store: a bounded map,
//!   lost on restart.
//! - [`TaskStore::Sqlite`] (`--task-db FILE`) persists every task, including
//!   its input, to a SQLite file. That is what lets a restart recover:
//!   - a task finished before the restart still answers `GET /api/agents/tasks/{id}`;
//!   - a task that was **working** when the process stopped is run again
//!     (at-least-once, up to [`MAX_ATTEMPTS`] starts, then failed by name);
//!   - a callback that was not yet delivered is delivered.
//!
//! The file is created `0600` on Unix: rows hold task inputs and callback
//! tokens.

use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{Map, Value};

use super::{Task, TaskState, Tasks};

/// Starts of one task before a restart gives up on it.
pub const MAX_ATTEMPTS: i64 = 3;
/// Finished rows kept in the file; the oldest finished ones are deleted first.
pub const MAX_DURABLE_TASKS: i64 = 10_000;

/// What a run needs, as persisted.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskInput {
    pub agent: String,
    pub args: Map<String, Value>,
    pub result_step: Option<String>,
    pub callback: Option<(String, String)>,
}

/// A task to resume after a restart.
#[derive(Debug, Clone, PartialEq)]
pub enum Recovered {
    /// It was working: run it again (this is start number `attempt`).
    Rerun { id: String, input: TaskInput, attempt: i64 },
    /// It finished, but its callback was never acknowledged: deliver `view`.
    Redeliver {
        id: String,
        url: String,
        token: String,
        view: Value,
    },
}

pub(crate) enum TaskStore {
    Memory(Mutex<Tasks>),
    Sqlite(Mutex<Connection>),
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS serve_task (
    id               TEXT PRIMARY KEY,
    agent            TEXT NOT NULL,
    args             TEXT NOT NULL,
    result_step      TEXT,
    callback_url     TEXT,
    callback_token   TEXT,
    state            TEXT NOT NULL CHECK (state IN ('working','completed','failed')),
    result           TEXT,
    error            TEXT,
    attempts         INTEGER NOT NULL DEFAULT 1,
    callback_done    INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS serve_task_state ON serve_task(state, updated_at);
";

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn row_state(state: &str, result: Option<String>, error: Option<String>) -> TaskState {
    match state {
        "completed" => TaskState::Completed(
            result
                .and_then(|r| serde_json::from_str(&r).ok())
                .unwrap_or(Value::Null),
        ),
        "failed" => TaskState::Failed(error.unwrap_or_default()),
        _ => TaskState::Working,
    }
}

impl TaskStore {
    pub fn memory() -> Self {
        TaskStore::Memory(Mutex::new(Tasks::default()))
    }

    /// Open (creating if needed) the SQLite store at `path`.
    pub fn open_sqlite(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        }
        let existed = path.exists();
        let conn = Connection::open(path).with_context(|| format!("open task db {}", path.display()))?;
        #[cfg(unix)]
        if !existed {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
        #[cfg(not(unix))]
        let _ = existed;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // FULL: a 202 for a task means the row is on disk.
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(SCHEMA).context("task db schema")?;
        Ok(TaskStore::Sqlite(Mutex::new(conn)))
    }

    /// Record a new working task.
    pub fn insert(&self, id: &str, input: &TaskInput) -> Result<()> {
        match self {
            TaskStore::Memory(m) => {
                m.lock().unwrap().insert(
                    id.to_string(),
                    Task {
                        agent: input.agent.clone(),
                        state: TaskState::Working,
                    },
                );
                Ok(())
            }
            TaskStore::Sqlite(c) => {
                let t = now();
                let (url, token) = match &input.callback {
                    Some((u, k)) => (Some(u.as_str()), Some(k.as_str())),
                    None => (None, None),
                };
                c.lock().unwrap().execute(
                    "INSERT INTO serve_task (id, agent, args, result_step, callback_url, callback_token, state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'working', ?7, ?7)",
                    params![
                        id,
                        input.agent,
                        Value::Object(input.args.clone()).to_string(),
                        input.result_step,
                        url,
                        token,
                        t
                    ],
                )?;
                Ok(())
            }
        }
    }

    /// Record a task's final state.
    pub fn finish(&self, id: &str, agent: &str, state: TaskState) -> Result<()> {
        match self {
            TaskStore::Memory(m) => {
                let mut tasks = m.lock().unwrap();
                let entry = tasks.by_id.entry(id.to_string()).or_insert(Task {
                    agent: agent.to_string(),
                    state: TaskState::Working,
                });
                entry.state = state;
                Ok(())
            }
            TaskStore::Sqlite(c) => {
                let (s, result, error) = match &state {
                    TaskState::Working => ("working", None, None),
                    TaskState::Completed(v) => ("completed", Some(v.to_string()), None),
                    TaskState::Failed(e) => ("failed", None, Some(e.clone())),
                };
                let conn = c.lock().unwrap();
                conn.execute(
                    "UPDATE serve_task SET state = ?2, result = ?3, error = ?4, updated_at = ?5 WHERE id = ?1",
                    params![id, s, result, error, now()],
                )?;
                // Retention: drop the oldest finished rows past the cap.
                conn.execute(
                    "DELETE FROM serve_task WHERE id IN (
                       SELECT id FROM serve_task WHERE state <> 'working'
                       ORDER BY updated_at ASC
                       LIMIT max(0, (SELECT count(*) FROM serve_task) - ?1))",
                    params![MAX_DURABLE_TASKS],
                )?;
                Ok(())
            }
        }
    }

    pub fn callback_delivered(&self, id: &str) -> Result<()> {
        if let TaskStore::Sqlite(c) = self {
            c.lock()
                .unwrap()
                .execute("UPDATE serve_task SET callback_done = 1 WHERE id = ?1", params![id])?;
        }
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Option<Task>> {
        match self {
            TaskStore::Memory(m) => Ok(m.lock().unwrap().by_id.get(id).cloned()),
            TaskStore::Sqlite(c) => Ok(c
                .lock()
                .unwrap()
                .query_row(
                    "SELECT agent, state, result, error FROM serve_task WHERE id = ?1",
                    params![id],
                    |r| {
                        Ok(Task {
                            agent: r.get(0)?,
                            state: row_state(&r.get::<_, String>(1)?, r.get(2)?, r.get(3)?),
                        })
                    },
                )
                .optional()?),
        }
    }

    /// Start of a process: the tasks a previous process left unfinished.
    ///
    /// Working rows are claimed for a rerun (their attempt count goes up
    /// first, so a task that crashes the process every time stops after
    /// [`MAX_ATTEMPTS`]); rows past the limit are failed by name.
    pub fn recover(&self) -> Result<Vec<Recovered>> {
        let TaskStore::Sqlite(c) = self else {
            return Ok(Vec::new());
        };
        let conn = c.lock().unwrap();
        let t = now();
        conn.execute(
            "UPDATE serve_task SET state = 'failed', updated_at = ?2,
               error = 'interrupted by a restart ' || attempts || ' time(s); not retried again (max ' || ?1 || ')'
             WHERE state = 'working' AND attempts >= ?1",
            params![MAX_ATTEMPTS, t],
        )?;
        conn.execute(
            "UPDATE serve_task SET attempts = attempts + 1, updated_at = ?1 WHERE state = 'working'",
            params![t],
        )?;
        let mut out = Vec::new();
        let mut q = conn.prepare(
            "SELECT id, agent, args, result_step, callback_url, callback_token, attempts
             FROM serve_task WHERE state = 'working' ORDER BY created_at",
        )?;
        let rows = q.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, i64>(6)?,
            ))
        })?;
        for row in rows {
            let (id, agent, args, result_step, url, token, attempt) = row?;
            let args = match serde_json::from_str::<Value>(&args) {
                Ok(Value::Object(m)) => m,
                _ => Map::new(),
            };
            out.push(Recovered::Rerun {
                id,
                input: TaskInput {
                    agent,
                    args,
                    result_step,
                    callback: url.zip(token),
                },
                attempt,
            });
        }
        drop(q);
        let mut q = conn.prepare(
            "SELECT id, agent, state, result, error, callback_url, callback_token
             FROM serve_task WHERE state <> 'working' AND callback_url IS NOT NULL AND callback_done = 0",
        )?;
        let rows = q.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Option<String>>(6)?,
            ))
        })?;
        for row in rows {
            let (id, agent, state, result, error, url, token) = row?;
            let task = Task {
                agent,
                state: row_state(&state, result, error),
            };
            out.push(Recovered::Redeliver {
                view: super::task_view(&id, &task),
                id,
                url,
                token: token.unwrap_or_default(),
            });
        }
        Ok(out)
    }
}

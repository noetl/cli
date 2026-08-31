//! Capability validation — checks whether a playbook's required
//! `executor.requires.{tools, features}` set is satisfied by a
//! runtime's advertised [`crate::playbook::RuntimeCapabilities`].
//!
//! Extracted from `repos/cli/src/playbook_runner.rs` lines 142-211
//! in R-1.1 PR-2b per § H.10.3 of Appendix H of the global hybrid
//! cloud blueprint.

use anyhow::Result;

use crate::playbook::{Playbook, RuntimeCapabilities};

/// Outcome of validating a playbook against runtime capabilities.
///
/// The validator returns a [`ValidationReport`] instead of just an
/// `anyhow::bail!` so the caller (CLI's `playbook_runner.rs` today,
/// the worker's load-playbook step tomorrow) can format the failure
/// against its own context — e.g. the CLI includes the playbook path
/// in the error message; the worker may include the execution_id.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub playbook_name: String,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<String>,
}

impl ValidationReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone)]
pub enum ValidationError {
    /// Playbook requires a different executor profile (e.g. "distributed").
    IncompatibleProfile { required: String },
    /// Playbook requires a tool kind the runtime does not support.
    MissingTool { tool: String, supported: Vec<String> },
    /// Playbook requires a feature the runtime does not support.
    MissingFeature { feature: String, supported: Vec<String> },
}

/// Validate that the given playbook's `executor.requires` set is
/// satisfied by the supplied runtime capabilities.  Returns a
/// [`ValidationReport`] describing every mismatch; the caller decides
/// whether to bail on the first error or report them all.
pub fn validate_capabilities(
    playbook: &Playbook,
    runtime: &RuntimeCapabilities,
) -> Result<ValidationReport> {
    let mut report = ValidationReport {
        playbook_name: playbook.metadata.name.clone(),
        errors: Vec::new(),
        warnings: Vec::new(),
    };

    let executor = match &playbook.executor {
        Some(e) => e,
        // No executor block — playbook makes no demands; runtime is fine.
        None => return Ok(report),
    };

    // Profile compatibility.
    match executor.profile.as_str() {
        "distributed" => {
            if runtime.runtime != "distributed" {
                report.errors.push(ValidationError::IncompatibleProfile {
                    required: "distributed".to_string(),
                });
            }
        }
        "local" | "auto" | "" => {
            // Compatible with both runtimes.
        }
        other => {
            report.warnings.push(format!(
                "Unknown executor profile '{}'; proceeding with '{}' runtime",
                other, runtime.runtime
            ));
        }
    }

    // Version compatibility.
    if executor.version != runtime.version && !executor.version.is_empty() {
        report.warnings.push(format!(
            "Playbook requires '{}', runtime provides '{}'.  Some features may not work as expected.",
            executor.version, runtime.version,
        ));
    }

    // Required tools + features.
    if let Some(requires) = &executor.requires {
        for tool in &requires.tools {
            if !runtime.tools.contains(tool) {
                report.errors.push(ValidationError::MissingTool {
                    tool: tool.clone(),
                    supported: runtime.tools.clone(),
                });
            }
        }
        for feature in &requires.features {
            if !runtime.features.contains(feature) {
                report.errors.push(ValidationError::MissingFeature {
                    feature: feature.clone(),
                    supported: runtime.features.clone(),
                });
            }
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::playbook::{Executor, ExecutorRequires, Metadata, Playbook};

    fn pb(executor: Option<Executor>) -> Playbook {
        Playbook {
            api_version: "noetl.io/v2".to_string(),
            kind: "Playbook".to_string(),
            metadata: Metadata {
                name: "test".to_string(),
                path: None,
            },
            executor,
            workload: None,
            keychain: None,
            workbook: None,
            workflow: Vec::new(),
        }
    }

    #[test]
    fn no_executor_block_is_ok() {
        let playbook = pb(None);
        let runtime = RuntimeCapabilities::local();
        let report = validate_capabilities(&playbook, &runtime).unwrap();
        assert!(report.is_ok());
    }

    #[test]
    fn distributed_profile_fails_against_local_runtime() {
        let playbook = pb(Some(Executor {
            profile: "distributed".to_string(),
            version: "".to_string(),
            requires: None,
            spec: None,
        }));
        let runtime = RuntimeCapabilities::local();
        let report = validate_capabilities(&playbook, &runtime).unwrap();
        assert!(!report.is_ok());
        assert!(matches!(
            report.errors[0],
            ValidationError::IncompatibleProfile { .. }
        ));
    }

    #[test]
    fn missing_tool_is_an_error() {
        let playbook = pb(Some(Executor {
            profile: "local".to_string(),
            version: "noetl-runtime/1".to_string(),
            requires: Some(ExecutorRequires {
                tools: vec!["nonexistent".to_string()],
                features: Vec::new(),
            }),
            spec: None,
        }));
        let runtime = RuntimeCapabilities::local();
        let report = validate_capabilities(&playbook, &runtime).unwrap();
        assert!(!report.is_ok());
        assert!(matches!(report.errors[0], ValidationError::MissingTool { .. }));
    }

    #[test]
    fn auto_profile_is_compatible_with_local() {
        let playbook = pb(Some(Executor {
            profile: "auto".to_string(),
            version: "".to_string(),
            requires: None,
            spec: None,
        }));
        let runtime = RuntimeCapabilities::local();
        let report = validate_capabilities(&playbook, &runtime).unwrap();
        assert!(report.is_ok());
    }
}

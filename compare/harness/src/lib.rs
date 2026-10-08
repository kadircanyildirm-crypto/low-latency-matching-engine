//! Shared code of the head-to-head comparison (`docs/COMPARISON.md`): the engine-neutral
//! command stream format and the scenarios recorded into it.

use std::path::PathBuf;

pub mod scenarios;
pub mod stream;

/// Where the recorded streams live: `$CMP_DATA`, or `compare/data`.
pub fn data_dir() -> PathBuf {
    std::env::var_os("CMP_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data"))
}

/// A count from the environment, `_` separators allowed.
pub fn env_count(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.replace('_', "").parse().ok())
        .unwrap_or(default)
}

/// Whether `$CMP_SCENARIOS` (a comma-separated list; unset means all) selects `name`.
pub fn scenario_selected(name: &str) -> bool {
    std::env::var("CMP_SCENARIOS")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .is_none_or(|names| names.split(',').any(|n| n.trim() == name))
}

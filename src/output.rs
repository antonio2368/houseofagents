use crate::error::AppError;
use crate::execution::ExecutionMode;
use chrono::{Local, Utc};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use toml::Value;

const ADJECTIVES: &[&str] = &[
    "swift", "bold", "calm", "dark", "keen", "warm", "pure", "vast", "wild", "soft", "deep",
    "pale", "thin", "cool", "fair", "grey", "high", "late", "lean", "long", "loud", "near",
    "open", "rare", "rich", "slow", "tall", "true", "wide", "worn", "blue", "cold", "firm",
    "flat", "full", "glad", "gold", "hard", "iron", "jade", "kind", "live", "mint", "neat",
    "pine", "real", "safe", "tame", "trim", "used", "wiry", "aged", "bent", "busy", "even",
    "fine", "free", "good", "hazy", "iced",
];

const NOUNS: &[&str] = &[
    "falcon", "river", "tiger", "stone", "grove", "flame", "frost", "cedar", "crane", "pearl",
    "maple", "ridge", "spark", "cloud", "field", "shore", "bloom", "drift", "ember", "forge",
    "glyph", "haven", "ivory", "knoll", "larch", "marsh", "noble", "olive", "prism", "quill",
    "realm", "shard", "thorn", "umbra", "vault", "watch", "yield", "birch", "delta", "flint",
    "grain", "heron", "inlet", "jetty", "kelp", "lodge", "mound", "nexus", "orbit", "plume",
    "quest", "roost", "slate", "trail", "aspen", "basin", "crest", "dune", "finch", "glint",
];

fn generate_random_name() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let adj = ADJECTIVES[rng.gen_range(0..ADJECTIVES.len())];
    let noun = NOUNS[rng.gen_range(0..NOUNS.len())];
    format!("{adj}-{noun}")
}

/// Check if a directory name matches the YYYY-MM-DD date dir pattern.
pub(crate) fn is_date_dir(name: &str) -> bool {
    name.len() == 10
        && name.as_bytes()[4] == b'-'
        && name.as_bytes()[7] == b'-'
        && name
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
}

/// Check if a leaf name looks like an old timestamp-based format.
/// Matches legacy flat (`YYYYMMDD_HHMMSS[_...]`) and old grouped (`HH-MM-SS[...]`).
fn is_old_format_leaf(leaf: &str) -> bool {
    is_legacy_flat(leaf) || is_old_timestamp_leaf(leaf)
}

/// Check if a leaf starts with the old HH-MM-SS timestamp pattern.
fn is_old_timestamp_leaf(leaf: &str) -> bool {
    leaf.len() >= 8
        && leaf.as_bytes()[2] == b'-'
        && leaf.as_bytes()[5] == b'-'
        && leaf[0..2].bytes().all(|b| b.is_ascii_digit())
        && leaf[3..5].bytes().all(|b| b.is_ascii_digit())
        && leaf[6..8].bytes().all(|b| b.is_ascii_digit())
}

/// Check if a directory name matches the legacy flat format: YYYYMMDD_HHMMSS[_...]
fn is_legacy_flat(dir_name: &str) -> bool {
    let mut parts = dir_name.splitn(3, '_');
    let date_part = match parts.next() {
        Some(p) if p.len() == 8 && p.bytes().all(|b| b.is_ascii_digit()) => p,
        _ => return false,
    };
    let time_part = match parts.next() {
        Some(p) if p.len() == 6 && p.bytes().all(|b| b.is_ascii_digit()) => p,
        _ => return false,
    };
    let _ = date_part;
    let _ = time_part;
    true
}

/// Approximate the creation time of a run directory.
///
/// Uses the mtime of `session.toml` or `batch.toml` (written once at run
/// creation and never modified) so that later writes of iteration files,
/// consolidation, or diagnostics into the directory do not alter the sort
/// order.  Falls back to the directory's own mtime when neither metadata
/// file exists (e.g. legacy layouts).
fn run_creation_time(path: &Path) -> SystemTime {
    let meta_file = path.join("session.toml");
    if let Ok(m) = std::fs::metadata(&meta_file) {
        if let Ok(t) = m.modified() {
            return t;
        }
    }
    let batch_file = path.join("batch.toml");
    if let Ok(m) = std::fs::metadata(&batch_file) {
        if let Ok(t) = m.modified() {
            return t;
        }
    }
    // Fallback: directory mtime (may shift if files are added later).
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

#[derive(Debug, Clone)]
pub struct OutputManager {
    run_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionInfo {
    pub name: Option<String>,
    pub mode: ExecutionMode,
    pub agents: Vec<String>,
    pub iterations: u32,
}

impl OutputManager {
    pub fn new(base_dir: &Path, session_name: Option<&str>) -> Result<Self, AppError> {
        Self::new_impl(base_dir, session_name, Local::now())
    }

    fn new_impl(
        base_dir: &Path,
        session_name: Option<&str>,
        _now: chrono::DateTime<Local>,
    ) -> Result<Self, AppError> {
        let date_part = _now.format("%Y-%m-%d").to_string();
        let date_dir = base_dir.join(&date_part);
        std::fs::create_dir_all(&date_dir)?;

        let run_dir = match session_name.map(|n| n.trim()).filter(|n| !n.is_empty()) {
            Some(name) => {
                let leaf = Self::sanitize_session_name(name);
                let candidate = date_dir.join(&leaf);
                match std::fs::create_dir(&candidate) {
                    Ok(()) => candidate,
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        return Err(AppError::Io(std::io::Error::new(
                            std::io::ErrorKind::AlreadyExists,
                            format!(
                                "Session '{}' already exists for {}. Choose a different name.",
                                name, date_part
                            ),
                        )));
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            None => {
                let mut attempts = 0;
                loop {
                    let leaf = generate_random_name();
                    let candidate = date_dir.join(&leaf);
                    match std::fs::create_dir(&candidate) {
                        Ok(()) => break candidate,
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                            attempts += 1;
                            if attempts >= 10 {
                                // Last resort: append numeric suffix. Use
                                // create_dir (not create_dir_all) so we never
                                // silently reuse an existing directory.
                                let fallback = format!("{leaf}-{attempts}");
                                let candidate = date_dir.join(&fallback);
                                std::fs::create_dir(&candidate)?;
                                break candidate;
                            }
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        };

        #[cfg(unix)]
        Self::update_latest_symlink(base_dir, &run_dir);
        Self::append_run_index(base_dir, &run_dir, session_name);

        Ok(Self { run_dir })
    }

    #[cfg(unix)]
    fn update_latest_symlink(base_dir: &Path, run_dir: &Path) {
        use std::os::unix::fs::symlink;
        let Ok(relative) = run_dir.strip_prefix(base_dir) else {
            return;
        };
        let link = base_dir.join("latest");
        let _ = std::fs::remove_file(&link);
        let _ = symlink(relative, &link);
    }

    fn append_run_index(base_dir: &Path, run_dir: &Path, session_name: Option<&str>) {
        use std::io::Write;
        let Ok(relative) = run_dir.strip_prefix(base_dir) else {
            return;
        };
        let relative_str = relative.to_string_lossy();
        let timestamp = Utc::now().to_rfc3339();

        let index_path = base_dir.join("runs.toml");
        let needs_sep = index_path.metadata().map(|m| m.len() > 0).unwrap_or(false);

        let path_val = toml::Value::String(relative_str.into_owned());
        let ts_val = toml::Value::String(timestamp);

        let mut block = String::new();
        if needs_sep {
            block.push('\n');
        }
        block.push_str(&format!(
            "[[entries]]\npath = {path_val}\ntimestamp = {ts_val}\n"
        ));
        if let Some(name) = session_name.filter(|n| !n.is_empty()) {
            let name_val = toml::Value::String(name.to_string());
            block.push_str(&format!("session_name = {name_val}\n"));
        }

        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)
            .and_then(|mut f| f.write_all(block.as_bytes()));
    }

    /// Enumerate all run directories under base_dir (legacy flat, old grouped
    /// with timestamps, and new grouped with plain names), returned newest-first
    /// by run creation time.
    pub(crate) fn scan_run_dirs(base_dir: &Path) -> Result<Vec<PathBuf>, AppError> {
        let mut entries: Vec<(SystemTime, PathBuf)> = Vec::new();

        for entry in std::fs::read_dir(base_dir)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            if !ft.is_dir() && !ft.is_symlink() {
                continue;
            }

            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str == "latest" {
                continue;
            }

            if ft.is_dir() && is_date_dir(&name_str) {
                // Scan subdirectories of date dir — accepts old HH-MM-SS leaves
                // and new plain-name leaves (anything that's a directory).
                for sub in std::fs::read_dir(entry.path())? {
                    let sub = sub?;
                    if !sub.file_type()?.is_dir() {
                        continue;
                    }
                    let path = sub.path();
                    let mtime = run_creation_time(&path);
                    entries.push((mtime, path));
                }
            } else if ft.is_dir() && is_legacy_flat(&name_str) {
                let path = entry.path();
                let mtime = run_creation_time(&path);
                entries.push((mtime, path));
            }
        }

        entries.sort_by(|a, b| b.0.cmp(&a.0)); // newest-first
        Ok(entries.into_iter().map(|(_, p)| p).collect())
    }

    pub fn new_batch_parent(base_dir: &Path, session_name: Option<&str>) -> Result<Self, AppError> {
        Self::new(base_dir, session_name)
    }

    pub fn from_existing(run_dir: PathBuf) -> Result<Self, AppError> {
        if !run_dir.is_dir() {
            return Err(AppError::Config(format!(
                "Run directory does not exist: {}",
                run_dir.display()
            )));
        }
        Ok(Self { run_dir })
    }

    pub fn sanitize_session_name(name: &str) -> String {
        name.chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    pub fn find_latest_session_run(
        base_dir: &Path,
        session_name: &str,
    ) -> Result<Option<PathBuf>, AppError> {
        if !base_dir.exists() {
            return Ok(None);
        }
        let sanitized = Self::sanitize_session_name(session_name);
        let old_suffix = format!("_{sanitized}");

        for path in Self::scan_run_dirs(base_dir)? {
            let leaf = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if Self::is_batch_root(&path) {
                continue;
            }
            // New format: leaf is exactly the sanitized session name
            if leaf == sanitized {
                return Ok(Some(path));
            }
            // Old format only: leaf has a timestamp prefix and ends with _session_name.
            // Restrict to old-format leaves so a new-format dir like "foo_alpha"
            // is not incorrectly matched when searching for "alpha".
            if is_old_format_leaf(leaf) && leaf.ends_with(&old_suffix) {
                return Ok(Some(path));
            }
        }
        Ok(None)
    }

    pub fn run_dir(&self) -> &PathBuf {
        &self.run_dir
    }

    pub fn read_agent_session_info(run_dir: &Path) -> Result<AgentSessionInfo, AppError> {
        let content = std::fs::read_to_string(run_dir.join("session.toml"))?;
        let value = content
            .parse::<Value>()
            .map_err(|e| AppError::Config(format!("Failed to parse session info: {e}")))?;

        let mode = match value.get("mode").and_then(|v| v.as_str()) {
            Some("relay") => ExecutionMode::Relay,
            Some("swarm") => ExecutionMode::Swarm,
            Some("solo") => ExecutionMode::Swarm,
            Some("pipeline") => ExecutionMode::Pipeline,
            Some(other) => {
                return Err(AppError::Config(format!(
                    "Unknown session mode in session info: {other}"
                )));
            }
            None => return Err(AppError::Config("Missing mode in session info".to_string())),
        };

        let agents = value
            .get("agents")
            .and_then(|v| v.as_array())
            .ok_or_else(|| AppError::Config("Missing agents in session info".to_string()))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| AppError::Config("Invalid agent entry in session info".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let iterations = value
            .get("iterations")
            .and_then(|v| v.as_integer())
            .unwrap_or(0)
            .max(0) as u32;

        Ok(AgentSessionInfo {
            name: value
                .get("name")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            mode,
            agents,
            iterations,
        })
    }

    pub fn is_batch_root(path: &Path) -> bool {
        path.join("batch.toml").is_file()
    }

    pub fn new_run_subdir(&self, run_id: u32) -> Result<Self, AppError> {
        let run_dir = self.run_dir.join(format!("run_{run_id}"));
        std::fs::create_dir_all(&run_dir)?;
        Ok(Self { run_dir })
    }

    pub fn write_prompt(&self, prompt: &str) -> Result<(), AppError> {
        let path = self.run_dir.join("prompt.md");
        std::fs::write(path, prompt)?;
        Ok(())
    }

    /// Write session metadata. agents is a list of (agent_name, provider_kind_key) pairs.
    pub fn write_session_info(
        &self,
        mode: &ExecutionMode,
        agents: &[(String, String)],
        iterations: u32,
        session_name: Option<&str>,
        models: &[(String, String)],
    ) -> Result<(), AppError> {
        let mut root = toml::map::Map::new();
        if let Some(name) = session_name.filter(|n| !n.is_empty()) {
            root.insert("name".into(), Value::String(name.to_string()));
        }
        root.insert("mode".into(), Value::String(mode.as_str().to_string()));
        root.insert(
            "agents".into(),
            Value::Array(
                agents
                    .iter()
                    .map(|(name, _)| Value::String(name.clone()))
                    .collect(),
            ),
        );
        root.insert("iterations".into(), Value::Integer(iterations as i64));

        let mut model_table = toml::map::Map::new();
        for (agent_name, model) in models {
            let key = Self::sanitize_session_name(agent_name);
            model_table.insert(key, Value::String(model.clone()));
        }
        root.insert("models".into(), Value::Table(model_table));

        let content = toml::to_string_pretty(&Value::Table(root))
            .map_err(|e| AppError::Config(format!("Failed to serialize session info: {e}")))?;
        let path = self.run_dir.join("session.toml");
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Write agent output file using sanitized agent name
    pub fn write_agent_output(
        &self,
        agent_name: &str,
        iteration: u32,
        content: &str,
    ) -> Result<PathBuf, AppError> {
        let sanitized = Self::sanitize_session_name(agent_name);
        let filename = format!("{}_iter{}.md", sanitized, iteration);
        let path = self.run_dir.join(&filename);
        std::fs::write(&path, content)?;
        Ok(path)
    }

    #[allow(dead_code)]
    pub fn write_block_output(
        &self,
        block_id: u32,
        block_name: &str,
        agent_name: &str,
        iteration: u32,
        content: &str,
    ) -> Result<PathBuf, AppError> {
        let name_key = if block_name.trim().is_empty() {
            format!("block{}", block_id)
        } else {
            format!("{}_b{}", Self::sanitize_session_name(block_name), block_id)
        };
        let sanitized = Self::sanitize_session_name(agent_name);
        let filename = format!("{}_{}_iter{}.md", name_key, sanitized, iteration);
        let path = self.run_dir.join(&filename);
        std::fs::write(&path, content)?;
        Ok(path)
    }

    pub fn write_pipeline_session_info(
        &self,
        block_count: usize,
        connection_count: usize,
        iterations: u32,
        pipeline_source: Option<&str>,
    ) -> Result<(), AppError> {
        let mut root = toml::map::Map::new();
        root.insert("mode".into(), Value::String("pipeline".to_string()));
        root.insert("blocks".into(), Value::Integer(block_count as i64));
        root.insert(
            "connections".into(),
            Value::Integer(connection_count as i64),
        );
        root.insert("iterations".into(), Value::Integer(iterations as i64));
        if let Some(src) = pipeline_source {
            root.insert("pipeline_source".into(), Value::String(src.to_string()));
        }
        let content = toml::to_string_pretty(&Value::Table(root))
            .map_err(|e| AppError::Config(format!("Failed to serialize session info: {e}")))?;
        let path = self.run_dir.join("session.toml");
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn write_batch_info(
        &self,
        runs: u32,
        concurrency: u32,
        mode: &ExecutionMode,
        agents: &[String],
        iterations: u32,
    ) -> Result<(), AppError> {
        let mut root = toml::map::Map::new();
        root.insert("runs".into(), Value::Integer(runs as i64));
        root.insert("concurrency".into(), Value::Integer(concurrency as i64));
        root.insert("mode".into(), Value::String(mode.as_str().to_string()));
        root.insert(
            "agents".into(),
            Value::Array(agents.iter().cloned().map(Value::String).collect()),
        );
        root.insert("iterations".into(), Value::Integer(iterations as i64));
        root.insert("timestamp".into(), Value::String(Utc::now().to_rfc3339()));

        let content = toml::to_string_pretty(&Value::Table(root))
            .map_err(|e| AppError::Config(format!("Failed to serialize batch info: {e}")))?;
        std::fs::write(self.run_dir.join("batch.toml"), content)?;
        Ok(())
    }

    pub fn append_error(&self, error: &str) -> Result<(), AppError> {
        use std::io::Write;
        let path = self.run_dir.join("_errors.log");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let ts = Local::now().format("%H:%M:%S");
        writeln!(file, "[{ts}] {error}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn sanitize_session_name_replaces_invalid_chars() {
        assert_eq!(
            OutputManager::sanitize_session_name("hello world/test:1"),
            "hello_world_test_1"
        );
    }

    #[test]
    fn sanitize_session_name_keeps_alnum_dash_underscore() {
        assert_eq!(
            OutputManager::sanitize_session_name("abc-XYZ_123"),
            "abc-XYZ_123"
        );
    }

    #[test]
    fn new_creates_run_dir_without_session_name() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), None).expect("new");
        assert!(mgr.run_dir().exists());
        assert!(mgr.run_dir().starts_with(base.path()));
        // Parent should be a date dir
        let parent_name = mgr
            .run_dir()
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or("");
        assert!(
            is_date_dir(parent_name),
            "parent should be a date dir: {parent_name}"
        );
        // Leaf should match adjective-noun pattern
        let leaf = mgr
            .run_dir()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        let parts: Vec<&str> = leaf.split('-').collect();
        assert_eq!(parts.len(), 2, "leaf should be adjective-noun: {leaf}");
        assert!(
            ADJECTIVES.contains(&parts[0]),
            "first word should be an adjective: {leaf}"
        );
        assert!(
            NOUNS.contains(&parts[1]),
            "second word should be a noun: {leaf}"
        );
    }

    #[test]
    fn new_creates_run_dir_with_sanitized_session_name() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), Some("my session")).expect("new");
        let leaf = mgr
            .run_dir()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        assert_eq!(leaf, "my_session");
        let parent_name = mgr
            .run_dir()
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or("");
        assert!(is_date_dir(parent_name));
    }

    #[test]
    fn from_existing_accepts_directory() {
        let base = tempdir().expect("tempdir");
        let dir = base.path().join("run");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let mgr = OutputManager::from_existing(dir.clone()).expect("from_existing");
        assert_eq!(mgr.run_dir(), &dir);
    }

    #[test]
    fn from_existing_rejects_missing_directory() {
        let base = tempdir().expect("tempdir");
        let dir = base.path().join("missing");
        let err = match OutputManager::from_existing(dir) {
            Ok(_) => panic!("should fail"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("Run directory does not exist"));
    }

    #[test]
    fn find_latest_session_run_returns_none_when_base_missing() {
        let base = tempdir().expect("tempdir");
        let missing = base.path().join("none");
        let found = OutputManager::find_latest_session_run(&missing, "abc").expect("find");
        assert!(found.is_none());
    }

    #[test]
    fn find_latest_session_run_finds_new_format() {
        let base = tempdir().expect("tempdir");
        // New format: leaf is the session name directly
        let p1 = base.path().join("2026-01-01/alpha");
        std::fs::create_dir_all(&p1).expect("mkdir");

        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        assert_eq!(found, p1);
    }

    #[test]
    fn find_latest_session_run_finds_old_format() {
        let base = tempdir().expect("tempdir");
        // Old grouped format with timestamp prefix
        let p1 = base.path().join("2026-04-01/00-00-00_alpha");
        std::fs::create_dir_all(&p1).expect("mkdir");

        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        assert_eq!(found, p1);
    }

    #[test]
    fn find_latest_session_run_finds_legacy_format() {
        let base = tempdir().expect("tempdir");
        let p1 = base.path().join("20260101_000000_111_alpha");
        std::fs::create_dir_all(&p1).expect("mkdir");

        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        assert_eq!(found, p1);
    }

    #[test]
    fn find_latest_session_run_skips_batch_roots() {
        let base = tempdir().expect("tempdir");
        let batch_root = base.path().join("2026-03-01/alpha");
        std::fs::create_dir_all(&batch_root).expect("mkdir");
        std::fs::write(batch_root.join("batch.toml"), "runs = 2").expect("write batch");

        let single = base.path().join("2026-02-01/alpha");
        std::fs::create_dir_all(&single).expect("mkdir");

        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        assert_eq!(found, single);
    }

    #[test]
    fn new_run_subdir_creates_child_directory() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new_batch_parent(base.path(), Some("batch")).expect("batch");
        let child = mgr.new_run_subdir(3).expect("run subdir");
        assert_eq!(child.run_dir(), &mgr.run_dir().join("run_3"));
        assert!(child.run_dir().exists());
    }

    #[test]
    fn write_batch_info_writes_expected_fields() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new_batch_parent(base.path(), Some("batch")).expect("batch");
        mgr.write_batch_info(
            5,
            3,
            &ExecutionMode::Relay,
            &["Claude".into(), "OpenAI".into()],
            2,
        )
        .expect("batch info");

        let content = std::fs::read_to_string(mgr.run_dir().join("batch.toml")).expect("read");
        let value = content.parse::<Value>().expect("toml");
        assert_eq!(value["runs"].as_integer(), Some(5));
        assert_eq!(value["concurrency"].as_integer(), Some(3));
        assert_eq!(value["mode"].as_str(), Some("relay"));
        assert_eq!(value["iterations"].as_integer(), Some(2));
    }

    #[test]
    fn write_prompt_creates_prompt_file() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), Some("s")).expect("new");
        mgr.write_prompt("hello prompt").expect("write");
        let content = std::fs::read_to_string(mgr.run_dir().join("prompt.md")).expect("read");
        assert_eq!(content, "hello prompt");
    }

    #[test]
    fn write_session_info_writes_expected_fields() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), Some("sess")).expect("new");
        mgr.write_session_info(
            &ExecutionMode::Relay,
            &[
                ("Claude".to_string(), "anthropic".to_string()),
                ("OpenAI".to_string(), "openai".to_string()),
            ],
            3,
            Some("sess"),
            &[
                ("Claude".to_string(), "claude-model".to_string()),
                ("OpenAI".to_string(), "gpt-model".to_string()),
            ],
        )
        .expect("write");

        let content = std::fs::read_to_string(mgr.run_dir().join("session.toml")).expect("read");
        let value = content.parse::<Value>().expect("toml parse");
        assert_eq!(value["name"].as_str(), Some("sess"));
        assert_eq!(value["mode"].as_str(), Some("relay"));
        assert_eq!(value["iterations"].as_integer(), Some(3));
        let agents = value["agents"].as_array().expect("agents");
        assert_eq!(agents.len(), 2);
        assert!(agents.iter().any(|v| v.as_str() == Some("Claude")));
        assert!(agents.iter().any(|v| v.as_str() == Some("OpenAI")));
        assert_eq!(value["models"]["Claude"].as_str(), Some("claude-model"));
        assert_eq!(value["models"]["OpenAI"].as_str(), Some("gpt-model"));
    }

    #[test]
    fn write_agent_output_writes_file_with_expected_name() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), None).expect("new");
        let path = mgr
            .write_agent_output("Gemini", 7, "answer")
            .expect("write");
        assert!(path.ends_with("Gemini_iter7.md"));
        let content = std::fs::read_to_string(path).expect("read");
        assert_eq!(content, "answer");
    }

    #[test]
    fn write_agent_output_sanitizes_name() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), None).expect("new");
        let path = mgr
            .write_agent_output("Claude Analyzer", 1, "answer")
            .expect("write");
        assert!(path.ends_with("Claude_Analyzer_iter1.md"));
    }

    #[test]
    fn append_error_appends_multiple_lines() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), None).expect("new");
        mgr.append_error("one").expect("append");
        mgr.append_error("two").expect("append");
        let content = std::fs::read_to_string(mgr.run_dir().join("_errors.log")).expect("read");
        assert!(content.contains("one"));
        assert!(content.contains("two"));
        assert_eq!(content.lines().count(), 2);
    }

    #[test]
    fn read_agent_session_info_parses_expected_fields() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), Some("sess")).expect("new");
        mgr.write_session_info(
            &ExecutionMode::Relay,
            &[
                ("Claude".to_string(), "anthropic".to_string()),
                ("OpenAI".to_string(), "openai".to_string()),
            ],
            2,
            Some("sess"),
            &[],
        )
        .expect("write");

        let session = OutputManager::read_agent_session_info(mgr.run_dir()).expect("session");
        assert_eq!(session.name.as_deref(), Some("sess"));
        assert_eq!(session.mode, ExecutionMode::Relay);
        assert_eq!(
            session.agents,
            vec!["Claude".to_string(), "OpenAI".to_string()]
        );
        assert_eq!(session.iterations, 2);
    }

    #[test]
    fn read_agent_session_info_maps_legacy_solo_to_swarm() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), Some("legacy-solo")).expect("new");
        // Write a session.toml with mode = "solo" to simulate a pre-removal session
        let content = "mode = \"solo\"\nagents = [\"Claude\", \"OpenAI\"]\niterations = 1\n";
        std::fs::write(mgr.run_dir().join("session.toml"), content).expect("write");

        let session = OutputManager::read_agent_session_info(mgr.run_dir()).expect("session");
        assert_eq!(session.mode, ExecutionMode::Swarm);
        assert_eq!(
            session.agents,
            vec!["Claude".to_string(), "OpenAI".to_string()]
        );
        assert_eq!(session.iterations, 1);
    }

    #[test]
    fn read_agent_session_info_rejects_pipeline_shape_without_agents() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), Some("pipeline")).expect("new");
        mgr.write_pipeline_session_info(1, 0, 1, None)
            .expect("write");

        let err = OutputManager::read_agent_session_info(mgr.run_dir()).expect_err("should fail");
        assert!(err.to_string().contains("Missing agents"));
    }

    // --- is_date_dir tests ---

    #[test]
    fn is_date_dir_valid() {
        assert!(is_date_dir("2026-03-09"));
        assert!(is_date_dir("2000-01-01"));
        assert!(is_date_dir("9999-12-31"));
    }

    #[test]
    fn is_date_dir_invalid() {
        assert!(!is_date_dir("20260309"));
        assert!(!is_date_dir("2026-3-9"));
        assert!(!is_date_dir("abcd-ef-gh"));
        assert!(!is_date_dir("latest"));
        assert!(!is_date_dir(""));
        assert!(!is_date_dir("2026-03-09x"));
    }

    // --- new_impl tests ---

    #[test]
    fn new_impl_session_name_becomes_leaf() {
        use chrono::TimeZone;
        let base = tempdir().expect("tempdir");
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let mgr = OutputManager::new_impl(base.path(), Some("my_run"), now).expect("new_impl");
        assert_eq!(
            mgr.run_dir(),
            &base.path().join("2026-01-01").join("my_run")
        );
    }

    #[test]
    fn new_impl_random_name_when_no_session() {
        use chrono::TimeZone;
        let base = tempdir().expect("tempdir");
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let mgr = OutputManager::new_impl(base.path(), None, now).expect("new_impl");
        let leaf = mgr
            .run_dir()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        let parts: Vec<&str> = leaf.split('-').collect();
        assert_eq!(parts.len(), 2, "leaf should be adjective-noun: {leaf}");
        assert!(ADJECTIVES.contains(&parts[0]));
        assert!(NOUNS.contains(&parts[1]));
    }

    #[test]
    fn new_impl_duplicate_session_returns_error() {
        use chrono::TimeZone;
        let base = tempdir().expect("tempdir");
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        std::fs::create_dir_all(base.path().join("2026-01-01/my_session")).expect("mkdir");
        let err = OutputManager::new_impl(base.path(), Some("my_session"), now)
            .expect_err("should fail");
        assert!(err.to_string().contains("already exists"));
    }

    // --- symlink tests ---

    #[cfg(unix)]
    #[test]
    fn latest_symlink_created() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), None).expect("new");
        let link = base.path().join("latest");
        assert!(link.exists(), "latest symlink should exist");
        let target = std::fs::read_link(&link).expect("readlink");
        assert!(target.is_relative(), "symlink should be relative");
        assert_eq!(base.path().join(&target), *mgr.run_dir());
    }

    #[cfg(unix)]
    #[test]
    fn latest_symlink_updated() {
        let base = tempdir().expect("tempdir");
        let _mgr1 = OutputManager::new(base.path(), Some("first")).expect("new");
        let mgr2 = OutputManager::new(base.path(), Some("second")).expect("new");
        let link = base.path().join("latest");
        let target = std::fs::read_link(&link).expect("readlink");
        assert_eq!(base.path().join(&target), *mgr2.run_dir());
    }

    // --- runs.toml tests ---

    #[test]
    fn runs_toml_single_entry() {
        let base = tempdir().expect("tempdir");
        let _mgr = OutputManager::new(base.path(), Some("sess")).expect("new");
        let content = std::fs::read_to_string(base.path().join("runs.toml")).expect("read");
        let parsed: toml::Value = content.parse().expect("parse");
        let entries = parsed["entries"].as_array().expect("entries array");
        assert_eq!(entries.len(), 1);
        let path = entries[0]["path"].as_str().expect("path");
        assert!(path.contains("sess"), "path should contain session name");
    }

    #[test]
    fn runs_toml_two_entries() {
        let base = tempdir().expect("tempdir");
        let _mgr1 = OutputManager::new(base.path(), Some("a")).expect("new");
        let _mgr2 = OutputManager::new(base.path(), Some("b")).expect("new");
        let content = std::fs::read_to_string(base.path().join("runs.toml")).expect("read");
        let parsed: toml::Value = content.parse().expect("parse");
        let entries = parsed["entries"].as_array().expect("entries array");
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn runs_toml_no_leading_newline() {
        let base = tempdir().expect("tempdir");
        let _mgr = OutputManager::new(base.path(), None).expect("new");
        let content = std::fs::read_to_string(base.path().join("runs.toml")).expect("read");
        assert!(
            content.starts_with('['),
            "first byte should be '[', got: {content:?}"
        );
    }

    #[test]
    fn runs_toml_omits_session_name_when_none() {
        let base = tempdir().expect("tempdir");
        let _mgr = OutputManager::new(base.path(), None).expect("new");
        let content = std::fs::read_to_string(base.path().join("runs.toml")).expect("read");
        assert!(
            !content.contains("session_name"),
            "should not contain session_name"
        );
    }

    // --- scan_run_dirs tests ---

    #[test]
    fn scan_run_dirs_handles_all_three_formats() {
        let base = tempdir().expect("tempdir");
        // Legacy flat
        std::fs::create_dir_all(base.path().join("20260101_120000_111")).expect("mkdir");
        // Old grouped with timestamp leaf
        std::fs::create_dir_all(base.path().join("2026-03-01/14-00-00")).expect("mkdir");
        // New grouped with plain name leaf
        std::fs::create_dir_all(base.path().join("2026-04-01/swift-falcon")).expect("mkdir");

        let dirs = OutputManager::scan_run_dirs(base.path()).expect("scan");
        assert_eq!(dirs.len(), 3);
    }

    #[test]
    fn scan_run_dirs_skips_junk() {
        let base = tempdir().expect("tempdir");
        std::fs::create_dir_all(base.path().join("random_dir")).expect("mkdir");
        std::fs::create_dir_all(base.path().join("20260101_120000_111")).expect("mkdir");
        let dirs = OutputManager::scan_run_dirs(base.path()).expect("scan");
        assert_eq!(dirs.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn scan_run_dirs_skips_latest() {
        use std::os::unix::fs::symlink;
        let base = tempdir().expect("tempdir");
        let real = base.path().join("2026-01-01/swift-falcon");
        std::fs::create_dir_all(&real).expect("mkdir");
        symlink("2026-01-01/swift-falcon", base.path().join("latest")).expect("symlink");
        let dirs = OutputManager::scan_run_dirs(base.path()).expect("scan");
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0], real);
    }

    // --- find_latest_session_run with new format ---

    #[test]
    fn find_session_run_new_format_exact_match() {
        let base = tempdir().expect("tempdir");
        std::fs::create_dir_all(base.path().join("2026-01-01/alpha")).expect("mkdir");
        std::fs::create_dir_all(base.path().join("2026-02-01/alpha")).expect("mkdir");
        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        // Should find one of the two "alpha" dirs (whichever is newest by mtime)
        let leaf = found.file_name().and_then(|n| n.to_str()).unwrap_or("");
        assert_eq!(leaf, "alpha");
    }

    #[test]
    fn find_session_run_old_format_backward_compat() {
        let base = tempdir().expect("tempdir");
        std::fs::create_dir_all(base.path().join("2026-01-01/10-00-00_alpha")).expect("mkdir");
        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        assert!(found.ends_with("10-00-00_alpha"));
    }

    #[test]
    fn find_session_run_legacy_backward_compat() {
        let base = tempdir().expect("tempdir");
        std::fs::create_dir_all(base.path().join("20260101_120000_111_alpha")).expect("mkdir");
        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        assert!(
            found
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with("_alpha")
        );
    }

    #[test]
    fn find_session_run_does_not_shadow_with_new_format_suffix() {
        let base = tempdir().expect("tempdir");
        // Create a new-format dir "foo_alpha" (newer mtime) and the real "alpha" (older)
        std::fs::create_dir_all(base.path().join("2026-01-01/alpha")).expect("mkdir");
        std::fs::create_dir_all(base.path().join("2026-02-01/foo_alpha")).expect("mkdir");
        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        // Must return the exact "alpha" match, not "foo_alpha"
        let leaf = found.file_name().and_then(|n| n.to_str()).unwrap_or("");
        assert_eq!(leaf, "alpha");
    }

    // --- generate_random_name tests ---

    #[test]
    fn random_name_generated_when_no_session() {
        let name = generate_random_name();
        let parts: Vec<&str> = name.split('-').collect();
        assert_eq!(parts.len(), 2);
        assert!(ADJECTIVES.contains(&parts[0]));
        assert!(NOUNS.contains(&parts[1]));
    }

    #[test]
    fn duplicate_session_name_returns_error() {
        let base = tempdir().expect("tempdir");
        let _mgr1 = OutputManager::new(base.path(), Some("dup")).expect("first should succeed");
        let err = OutputManager::new(base.path(), Some("dup")).expect_err("second should fail");
        assert!(err.to_string().contains("already exists"));
    }
}

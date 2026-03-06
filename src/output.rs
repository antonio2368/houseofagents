use crate::error::AppError;
use crate::execution::ExecutionMode;
use chrono::Local;
use rand::Rng;
use std::path::{Path, PathBuf};
use toml::Value;

pub struct OutputManager {
    run_dir: PathBuf,
}

impl OutputManager {
    pub fn new(base_dir: &Path, session_name: Option<&str>) -> Result<Self, AppError> {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
        let suffix: u16 = rand::rng().random_range(100..999);
        let dir_name = match session_name {
            Some(name) if !name.is_empty() => {
                let sanitized = Self::sanitize_session_name(name);
                format!("{}_{}_{}", timestamp, suffix, sanitized)
            }
            _ => format!("{}_{}", timestamp, suffix),
        };
        let run_dir = base_dir.join(dir_name);
        std::fs::create_dir_all(&run_dir)?;
        Ok(Self { run_dir })
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
        let suffix = format!("_{}", Self::sanitize_session_name(session_name));
        let mut matches: Vec<(String, PathBuf)> = Vec::new();

        for entry in std::fs::read_dir(base_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(&suffix) {
                matches.push((name, entry.path()));
            }
        }

        matches.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(matches.into_iter().next().map(|(_, path)| path))
    }

    pub fn run_dir(&self) -> &PathBuf {
        &self.run_dir
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
        root.insert("connections".into(), Value::Integer(connection_count as i64));
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
    }

    #[test]
    fn new_creates_run_dir_with_sanitized_session_name() {
        let base = tempdir().expect("tempdir");
        let mgr = OutputManager::new(base.path(), Some("my session")).expect("new");
        let name = mgr
            .run_dir()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        assert!(name.ends_with("_my_session"));
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
    fn find_latest_session_run_picks_lexicographically_latest_match() {
        let base = tempdir().expect("tempdir");
        let p1 = base.path().join("20260101_000000_111_alpha");
        let p2 = base.path().join("20260201_000000_222_alpha");
        let p3 = base.path().join("20260301_000000_333_beta");
        std::fs::create_dir_all(&p1).expect("mkdir");
        std::fs::create_dir_all(&p2).expect("mkdir");
        std::fs::create_dir_all(&p3).expect("mkdir");

        let found = OutputManager::find_latest_session_run(base.path(), "alpha")
            .expect("find")
            .expect("some");
        assert_eq!(found, p2);
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
                ("Codex".to_string(), "openai".to_string()),
            ],
            3,
            Some("sess"),
            &[
                ("Claude".to_string(), "claude-model".to_string()),
                ("Codex".to_string(), "gpt-model".to_string()),
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
        assert!(agents.iter().any(|v| v.as_str() == Some("Codex")));
        assert_eq!(value["models"]["Claude"].as_str(), Some("claude-model"));
        assert_eq!(value["models"]["Codex"].as_str(), Some("gpt-model"));
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
}

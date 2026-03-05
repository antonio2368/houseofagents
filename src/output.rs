use crate::error::AppError;
use crate::execution::ExecutionMode;
use crate::provider::ProviderKind;
use chrono::Local;
use rand::Rng;
use std::path::{Path, PathBuf};
use toml::Value;

pub struct OutputManager {
    run_dir: PathBuf,
}

impl OutputManager {
    pub fn new(base_dir: &PathBuf, session_name: Option<&str>) -> Result<Self, AppError> {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
        let suffix: u16 = rand::thread_rng().gen_range(100..999);
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

    pub fn write_session_info(
        &self,
        mode: &ExecutionMode,
        agents: &[ProviderKind],
        iterations: u32,
        session_name: Option<&str>,
        models: &[(ProviderKind, String)],
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
                    .map(|a| Value::String(a.config_key().to_string()))
                    .collect(),
            ),
        );
        root.insert("iterations".into(), Value::Integer(iterations as i64));

        let mut model_table = toml::map::Map::new();
        for (kind, model) in models {
            model_table.insert(kind.config_key().to_string(), Value::String(model.clone()));
        }
        root.insert("models".into(), Value::Table(model_table));

        let content = toml::to_string_pretty(&Value::Table(root))
            .map_err(|e| AppError::Config(format!("Failed to serialize session info: {e}")))?;
        let path = self.run_dir.join("session.toml");
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn write_agent_output(
        &self,
        kind: ProviderKind,
        iteration: u32,
        content: &str,
    ) -> Result<PathBuf, AppError> {
        let filename = format!("{}_iter{}.md", kind.config_key(), iteration);
        let path = self.run_dir.join(&filename);
        std::fs::write(&path, content)?;
        Ok(path)
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

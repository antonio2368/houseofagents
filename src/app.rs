use crate::config::{AppConfig, ProviderConfig};
use crate::execution::{ExecutionMode, ProgressEvent};
use crate::provider::ProviderKind;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    Home,
    Prompt,
    Order,
    Running,
    Results,
}

pub struct App {
    pub config: AppConfig,
    pub config_path_override: Option<String>,
    pub session_overrides: HashMap<String, ProviderConfig>,
    pub session_diagnostic_overrides: HashMap<String, ProviderConfig>,
    pub session_http_timeout_seconds: Option<u64>,
    pub session_model_fetch_timeout_seconds: Option<u64>,
    pub session_cli_timeout_seconds: Option<u64>,
    pub screen: Screen,
    pub should_quit: bool,

    // Home screen state
    pub selected_agents: Vec<ProviderKind>,
    pub selected_mode: ExecutionMode,
    pub home_cursor: usize,
    pub home_section: HomeSection,

    // Prompt screen state
    pub prompt_text: String,
    pub prompt_cursor: usize,
    pub session_name: String,
    pub iterations: u32,
    pub iterations_buf: String,
    pub resume_previous: bool,
    pub prompt_focus: PromptFocus,

    // Order screen state (relay only)
    pub order_cursor: usize,
    pub order_grabbed: Option<usize>,

    // Running screen state
    pub progress_events: Vec<ProgressEvent>,
    pub is_running: bool,
    pub run_error: Option<String>,
    pub consolidation_active: bool,
    pub consolidation_phase: ConsolidationPhase,
    pub consolidation_provider_cursor: usize,
    pub consolidation_prompt: String,
    pub consolidation_running: bool,
    pub consolidation_rx: Option<mpsc::UnboundedReceiver<Result<String, String>>>,
    pub diagnostic_running: bool,
    pub diagnostic_rx: Option<mpsc::UnboundedReceiver<Result<String, String>>>,

    // Results screen state
    pub result_files: Vec<PathBuf>,
    pub result_cursor: usize,
    pub result_preview: String,

    // Edit popup
    pub show_edit_popup: bool,
    pub edit_popup_section: EditPopupSection,
    pub edit_popup_cursor: usize,
    pub edit_popup_diagnostic_cursor: usize,
    pub edit_popup_timeout_cursor: usize,
    pub edit_popup_field: EditField,
    pub edit_popup_editing: bool,
    pub edit_buffer: String,

    // Model picker (within edit popup)
    pub model_picker_active: bool,
    pub model_picker_loading: bool,
    pub model_picker_all_models: Vec<String>,
    pub model_picker_list: Vec<String>,
    pub model_picker_filter: String,
    pub model_picker_cursor: usize,
    pub model_picker_rx: Option<mpsc::UnboundedReceiver<Result<Vec<String>, String>>>,

    // Config save state (within edit popup)
    pub config_save_in_progress: bool,
    pub config_save_rx: Option<mpsc::UnboundedReceiver<Result<(), String>>>,

    // CLI availability per provider
    pub cli_available: HashMap<ProviderKind, bool>,

    // Help popup
    pub show_help_popup: bool,
    pub help_popup_scroll: u16,

    // Error modal
    pub error_modal: Option<String>,

    // Execution channel state (not part of UI state)
    pub progress_rx: Option<mpsc::UnboundedReceiver<ProgressEvent>>,
    pub cancel_flag: Arc<AtomicBool>,
    pub run_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeSection {
    Agents,
    Mode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptFocus {
    Text,
    SessionName,
    Iterations,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum EditField {
    ApiKey,
    Model,
    ExtraCliArgs,
    OutputDir,
    TimeoutSeconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditPopupSection {
    Providers,
    Diagnostics,
    Timeouts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationPhase {
    Confirm,
    Provider,
    Prompt,
}

fn detect_cli(name: &str) -> bool {
    std::process::Command::new("which")
        .arg(name)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

impl App {
    pub fn new(config: AppConfig) -> Self {
        let mut cli_available = HashMap::new();
        cli_available.insert(ProviderKind::Anthropic, detect_cli("claude"));
        cli_available.insert(ProviderKind::OpenAI, detect_cli("codex"));
        cli_available.insert(ProviderKind::Gemini, detect_cli("gemini"));

        // Auto-default: if no API key but CLI available, insert session override with use_cli
        let mut session_overrides = HashMap::new();
        for kind in ProviderKind::all() {
            let key = kind.config_key();
            let existing = config.providers.get(key).cloned();
            let has_key = existing
                .as_ref()
                .map(|c| !c.api_key.is_empty())
                .unwrap_or(false);
            let has_cli = cli_available.get(kind).copied().unwrap_or(false);
            if !has_key && has_cli {
                let mut cfg = existing.unwrap_or(ProviderConfig {
                    api_key: String::new(),
                    model: String::new(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    extra_cli_args: String::new(),
                });
                cfg.use_cli = true;
                session_overrides.insert(key.to_string(), cfg);
            }
        }

        Self {
            config,
            config_path_override: None,
            session_overrides,
            session_diagnostic_overrides: HashMap::new(),
            session_http_timeout_seconds: None,
            session_model_fetch_timeout_seconds: None,
            session_cli_timeout_seconds: None,
            screen: Screen::Home,
            should_quit: false,
            selected_agents: Vec::new(),
            selected_mode: ExecutionMode::Solo,
            home_cursor: 0,
            home_section: HomeSection::Agents,
            prompt_text: String::new(),
            prompt_cursor: 0,
            session_name: String::new(),
            iterations: 1,
            iterations_buf: "1".into(),
            resume_previous: false,
            prompt_focus: PromptFocus::Text,
            order_cursor: 0,
            order_grabbed: None,
            progress_events: Vec::new(),
            is_running: false,
            run_error: None,
            consolidation_active: false,
            consolidation_phase: ConsolidationPhase::Confirm,
            consolidation_provider_cursor: 0,
            consolidation_prompt: String::new(),
            consolidation_running: false,
            consolidation_rx: None,
            diagnostic_running: false,
            diagnostic_rx: None,
            result_files: Vec::new(),
            result_cursor: 0,
            result_preview: String::new(),
            show_edit_popup: false,
            edit_popup_section: EditPopupSection::Providers,
            edit_popup_cursor: 0,
            edit_popup_diagnostic_cursor: 0,
            edit_popup_timeout_cursor: 0,
            edit_popup_field: EditField::ApiKey,
            edit_popup_editing: false,
            edit_buffer: String::new(),
            model_picker_active: false,
            model_picker_loading: false,
            model_picker_all_models: Vec::new(),
            model_picker_list: Vec::new(),
            model_picker_filter: String::new(),
            model_picker_cursor: 0,
            model_picker_rx: None,
            config_save_in_progress: false,
            config_save_rx: None,
            cli_available,
            show_help_popup: false,
            help_popup_scroll: 0,
            error_modal: None,
            progress_rx: None,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            run_dir: None,
        }
    }

    pub fn available_providers(&self) -> Vec<(ProviderKind, bool)> {
        ProviderKind::all()
            .iter()
            .map(|&kind| {
                let config = self.effective_provider_config(kind);
                let has_key = config.map(|c| !c.api_key.is_empty()).unwrap_or(false);
                let using_cli = config.map(|c| c.use_cli).unwrap_or(false);
                let cli_ok = self.cli_available.get(&kind).copied().unwrap_or(false);
                let available = if using_cli { cli_ok } else { has_key };
                (kind, available)
            })
            .collect()
    }

    pub fn effective_provider_config(&self, kind: ProviderKind) -> Option<&ProviderConfig> {
        let key = kind.config_key();
        self.session_overrides
            .get(key)
            .or_else(|| self.config.providers.get(key))
    }

    pub fn effective_diagnostic_config(&self, kind: ProviderKind) -> Option<&ProviderConfig> {
        let key = kind.config_key();
        self.session_diagnostic_overrides
            .get(key)
            .or_else(|| self.config.diagnostics.get(key))
    }

    pub fn effective_http_timeout_seconds(&self) -> u64 {
        self.session_http_timeout_seconds
            .unwrap_or(self.config.http_timeout_seconds)
    }

    pub fn effective_model_fetch_timeout_seconds(&self) -> u64 {
        self.session_model_fetch_timeout_seconds
            .unwrap_or(self.config.model_fetch_timeout_seconds)
    }

    pub fn effective_cli_timeout_seconds(&self) -> u64 {
        self.session_cli_timeout_seconds
            .unwrap_or(self.config.cli_timeout_seconds)
    }

    pub fn toggle_agent(&mut self, kind: ProviderKind) {
        if let Some(pos) = self.selected_agents.iter().position(|&k| k == kind) {
            self.selected_agents.remove(pos);
        } else {
            self.selected_agents.push(kind);
        }
    }

    pub fn move_order_up(&mut self) {
        if self.order_cursor > 0 {
            if let Some(grabbed) = self.order_grabbed {
                self.selected_agents.swap(grabbed, grabbed - 1);
                self.order_grabbed = Some(grabbed - 1);
            }
            self.order_cursor -= 1;
        }
    }

    pub fn move_order_down(&mut self) {
        if self.order_cursor < self.selected_agents.len().saturating_sub(1) {
            if let Some(grabbed) = self.order_grabbed {
                self.selected_agents.swap(grabbed, grabbed + 1);
                self.order_grabbed = Some(grabbed + 1);
            }
            self.order_cursor += 1;
        }
    }
}

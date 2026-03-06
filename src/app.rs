use crate::config::{AgentConfig, AppConfig};
use crate::execution::pipeline::{BlockId, PipelineDefinition};
use crate::execution::{ExecutionMode, ProgressEvent};
use crate::output::OutputManager;
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
    Pipeline,
}

pub struct App {
    pub config: AppConfig,
    pub config_path_override: Option<String>,
    /// Session overrides keyed by agent name
    pub session_overrides: HashMap<String, AgentConfig>,
    pub session_http_timeout_seconds: Option<u64>,
    pub session_model_fetch_timeout_seconds: Option<u64>,
    pub session_cli_timeout_seconds: Option<u64>,
    pub screen: Screen,
    pub should_quit: bool,

    // Home screen state — selected agents by name
    pub selected_agents: Vec<String>,
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
    pub forward_prompt: bool,
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

    // CLI availability per provider kind
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

    // Pipeline builder — core
    pub pipeline_def: PipelineDefinition,
    pub pipeline_next_id: BlockId,
    pub pipeline_block_cursor: Option<BlockId>,
    pub pipeline_focus: PipelineFocus,
    pub pipeline_canvas_offset: (i16, i16),

    // Pipeline prompt/session/iterations
    pub pipeline_prompt_cursor: usize,
    pub pipeline_session_name: String,
    pub pipeline_iterations_buf: String,

    // Pipeline connect mode
    pub pipeline_connecting_from: Option<BlockId>,

    // Pipeline remove-connection mode
    pub pipeline_removing_conn: bool,
    pub pipeline_conn_cursor: usize,

    // Pipeline block edit popup
    pub pipeline_show_edit: bool,
    pub pipeline_edit_field: PipelineEditField,
    pub pipeline_edit_name_buf: String,
    pub pipeline_edit_name_cursor: usize,
    pub pipeline_edit_agent_idx: usize,
    pub pipeline_edit_prompt_buf: String,
    pub pipeline_edit_prompt_cursor: usize,
    pub pipeline_edit_session_buf: String,
    pub pipeline_edit_session_cursor: usize,

    // Pipeline file dialog
    pub pipeline_file_dialog: Option<PipelineDialogMode>,
    pub pipeline_file_input: String,
    pub pipeline_file_list: Vec<String>,
    pub pipeline_file_cursor: usize,

    // Pipeline save state
    pub pipeline_save_path: Option<PathBuf>,
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
    Resume,
    ForwardPrompt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum EditField {
    ApiKey,
    Model,
    ExtraCliArgs,
    OutputDir,
    TimeoutSeconds,
    AgentName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditPopupSection {
    Providers,
    Timeouts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationPhase {
    Confirm,
    Provider,
    Prompt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineFocus {
    InitialPrompt,
    SessionName,
    Iterations,
    Builder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineEditField {
    Name,
    Agent,
    Prompt,
    SessionId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineDialogMode {
    Save,
    Load,
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
        for agent in &config.agents {
            let has_key = !agent.api_key.is_empty();
            let has_cli = cli_available.get(&agent.provider).copied().unwrap_or(false);
            if !has_key && has_cli {
                let mut override_agent = agent.clone();
                override_agent.use_cli = true;
                session_overrides.insert(agent.name.clone(), override_agent);
            }
        }

        Self {
            config,
            config_path_override: None,
            session_overrides,
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
            forward_prompt: false,
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
            pipeline_def: PipelineDefinition::default(),
            pipeline_next_id: 1,
            pipeline_block_cursor: None,
            pipeline_focus: PipelineFocus::InitialPrompt,
            pipeline_canvas_offset: (0, 0),
            pipeline_prompt_cursor: 0,
            pipeline_session_name: String::new(),
            pipeline_iterations_buf: "1".into(),
            pipeline_connecting_from: None,
            pipeline_removing_conn: false,
            pipeline_conn_cursor: 0,
            pipeline_show_edit: false,
            pipeline_edit_field: PipelineEditField::Name,
            pipeline_edit_name_buf: String::new(),
            pipeline_edit_name_cursor: 0,
            pipeline_edit_agent_idx: 0,
            pipeline_edit_prompt_buf: String::new(),
            pipeline_edit_prompt_cursor: 0,
            pipeline_edit_session_buf: String::new(),
            pipeline_edit_session_cursor: 0,
            pipeline_file_dialog: None,
            pipeline_file_input: String::new(),
            pipeline_file_list: Vec::new(),
            pipeline_file_cursor: 0,
            pipeline_save_path: None,
        }
    }

    /// Returns list of (agent_config, is_available) for all configured agents
    pub fn available_agents(&self) -> Vec<(&AgentConfig, bool)> {
        self.config
            .agents
            .iter()
            .map(|agent| {
                let config = self.effective_agent_config(&agent.name).unwrap_or(agent);
                let has_key = !config.api_key.is_empty();
                let using_cli = config.use_cli;
                let cli_ok = self.cli_available.get(&config.provider).copied().unwrap_or(false);
                let available = if using_cli { cli_ok } else { has_key };
                (agent, available)
            })
            .collect()
    }

    /// Get effective agent config by name (session override or global)
    pub fn effective_agent_config(&self, name: &str) -> Option<&AgentConfig> {
        self.session_overrides
            .get(name)
            .or_else(|| self.config.agents.iter().find(|a| a.name == name))
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

    pub fn toggle_agent(&mut self, name: &str) {
        if let Some(pos) = self.selected_agents.iter().position(|n| n == name) {
            self.selected_agents.remove(pos);
        } else {
            self.selected_agents.push(name.to_string());
        }
    }

    /// Get the sanitized filename key for an agent name
    pub fn agent_file_key(name: &str) -> String {
        OutputManager::sanitize_session_name(name)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AgentConfig;

    fn agent_cfg(name: &str, provider: ProviderKind, api_key: &str, use_cli: bool) -> AgentConfig {
        AgentConfig {
            name: name.to_string(),
            provider,
            api_key: api_key.to_string(),
            model: "m".to_string(),
            reasoning_effort: Some("medium".to_string()),
            thinking_effort: Some("medium".to_string()),
            use_cli,
            cli_print_mode: true,
            extra_cli_args: String::new(),
        }
    }

    fn base_config() -> AppConfig {
        let agents = vec![
            agent_cfg("Claude", ProviderKind::Anthropic, "k1", false),
            agent_cfg("Codex", ProviderKind::OpenAI, "k2", false),
            agent_cfg("Gemini", ProviderKind::Gemini, "", true),
        ];

        AppConfig {
            output_dir: "/tmp/out".to_string(),
            default_max_tokens: 4096,
            max_history_messages: 50,
            http_timeout_seconds: 120,
            model_fetch_timeout_seconds: 30,
            cli_timeout_seconds: 600,
            diagnostic_provider: None,
            agents,
            providers: HashMap::new(),
        }
    }

    fn app_with_known_cli() -> App {
        let mut app = App::new(base_config());
        app.cli_available.insert(ProviderKind::Anthropic, true);
        app.cli_available.insert(ProviderKind::OpenAI, false);
        app.cli_available.insert(ProviderKind::Gemini, true);
        app
    }

    #[test]
    fn app_new_initial_state() {
        let app = App::new(base_config());
        assert_eq!(app.screen, Screen::Home);
        assert!(!app.should_quit);
        assert!(app.selected_agents.is_empty());
        assert_eq!(app.selected_mode, crate::execution::ExecutionMode::Solo);
        assert_eq!(app.iterations, 1);
        assert_eq!(app.iterations_buf, "1");
        assert!(!app.is_running);
    }

    #[test]
    fn effective_agent_config_prefers_session_override() {
        let mut app = app_with_known_cli();
        app.session_overrides.insert(
            "Codex".to_string(),
            agent_cfg("Codex", ProviderKind::OpenAI, "override", true),
        );
        let cfg = app
            .effective_agent_config("Codex")
            .expect("config");
        assert_eq!(cfg.api_key, "override");
        assert!(cfg.use_cli);
    }

    #[test]
    fn effective_agent_config_falls_back_to_global() {
        let app = app_with_known_cli();
        let cfg = app
            .effective_agent_config("Claude")
            .expect("config");
        assert_eq!(cfg.api_key, "k1");
        assert!(!cfg.use_cli);
    }

    #[test]
    fn effective_timeout_values_use_global_defaults() {
        let app = app_with_known_cli();
        assert_eq!(app.effective_http_timeout_seconds(), 120);
        assert_eq!(app.effective_model_fetch_timeout_seconds(), 30);
        assert_eq!(app.effective_cli_timeout_seconds(), 600);
    }

    #[test]
    fn effective_timeout_values_use_session_overrides() {
        let mut app = app_with_known_cli();
        app.session_http_timeout_seconds = Some(9);
        app.session_model_fetch_timeout_seconds = Some(8);
        app.session_cli_timeout_seconds = Some(7);
        assert_eq!(app.effective_http_timeout_seconds(), 9);
        assert_eq!(app.effective_model_fetch_timeout_seconds(), 8);
        assert_eq!(app.effective_cli_timeout_seconds(), 7);
    }

    #[test]
    fn toggle_agent_adds_and_removes() {
        let mut app = app_with_known_cli();
        app.toggle_agent("Claude");
        assert_eq!(app.selected_agents, vec!["Claude"]);
        app.toggle_agent("Claude");
        assert!(app.selected_agents.is_empty());
    }

    #[test]
    fn move_order_up_without_grabbed_only_moves_cursor() {
        let mut app = app_with_known_cli();
        app.selected_agents = vec!["Claude".into(), "Codex".into(), "Gemini".into()];
        app.order_cursor = 2;
        app.move_order_up();
        assert_eq!(app.order_cursor, 1);
        assert_eq!(
            app.selected_agents,
            vec!["Claude", "Codex", "Gemini"]
        );
    }

    #[test]
    fn move_order_up_with_grabbed_swaps_agents() {
        let mut app = app_with_known_cli();
        app.selected_agents = vec!["Claude".into(), "Codex".into(), "Gemini".into()];
        app.order_cursor = 2;
        app.order_grabbed = Some(2);
        app.move_order_up();
        assert_eq!(
            app.selected_agents,
            vec!["Claude", "Gemini", "Codex"]
        );
        assert_eq!(app.order_cursor, 1);
        assert_eq!(app.order_grabbed, Some(1));
    }

    #[test]
    fn move_order_down_without_grabbed_only_moves_cursor() {
        let mut app = app_with_known_cli();
        app.selected_agents = vec!["Claude".into(), "Codex".into(), "Gemini".into()];
        app.order_cursor = 0;
        app.move_order_down();
        assert_eq!(app.order_cursor, 1);
        assert_eq!(
            app.selected_agents,
            vec!["Claude", "Codex", "Gemini"]
        );
    }

    #[test]
    fn move_order_down_with_grabbed_swaps_agents() {
        let mut app = app_with_known_cli();
        app.selected_agents = vec!["Claude".into(), "Codex".into(), "Gemini".into()];
        app.order_cursor = 0;
        app.order_grabbed = Some(0);
        app.move_order_down();
        assert_eq!(
            app.selected_agents,
            vec!["Codex", "Claude", "Gemini"]
        );
        assert_eq!(app.order_cursor, 1);
        assert_eq!(app.order_grabbed, Some(1));
    }

    #[test]
    fn available_agents_api_requires_key() {
        let mut app = app_with_known_cli();
        app.session_overrides.insert(
            "Claude".to_string(),
            agent_cfg("Claude", ProviderKind::Anthropic, "", false),
        );
        let agents = app.available_agents();
        let claude = agents.iter().find(|(a, _)| a.name == "Claude").unwrap();
        assert!(!claude.1);
    }

    #[test]
    fn available_agents_cli_requires_installed_cli() {
        let mut app = app_with_known_cli();
        app.session_overrides.insert(
            "Codex".to_string(),
            agent_cfg("Codex", ProviderKind::OpenAI, "", true),
        );
        app.cli_available.insert(ProviderKind::OpenAI, false);
        let agents = app.available_agents();
        let codex = agents.iter().find(|(a, _)| a.name == "Codex").unwrap();
        assert!(!codex.1);
    }

    #[test]
    fn available_agents_cli_available_when_installed() {
        let mut app = app_with_known_cli();
        app.session_overrides.insert(
            "Gemini".to_string(),
            agent_cfg("Gemini", ProviderKind::Gemini, "", true),
        );
        app.cli_available.insert(ProviderKind::Gemini, true);
        let agents = app.available_agents();
        let gemini = agents.iter().find(|(a, _)| a.name == "Gemini").unwrap();
        assert!(gemini.1);
    }
}

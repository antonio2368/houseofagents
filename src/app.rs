use crate::config::{AgentConfig, AppConfig};
use crate::execution::pipeline::{BlockId, PipelineDefinition};
use crate::execution::{BatchProgressEvent, ExecutionMode, ProgressEvent, PromptRuntimeContext};
use crate::output::OutputManager;
use crate::provider::ProviderKind;
use reqwest::Client;
use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const ACTIVITY_LOG_LIMIT: usize = 200;
const ERROR_LEDGER_LIMIT: usize = 200;
const ERROR_LEDGER_ENTRY_MAX_CHARS: usize = 4096;
const RECENT_ACTIVITY_LOG_LIMIT: usize = 10;
const RECENT_LOGS_CAP: usize = 20;

pub(crate) struct AgentTimer {
    pub started_at: Instant,
    pub finished_at: Option<Instant>,
}

impl AgentTimer {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            finished_at: None,
        }
    }
    pub fn finish(&mut self) {
        self.finished_at = Some(Instant::now());
    }
    pub fn elapsed(&self) -> Duration {
        self.finished_at
            .unwrap_or_else(Instant::now)
            .duration_since(self.started_at)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AgentRowStatus {
    Pending,
    Running,
    Finished,
    Error(String),
    Skipped(String),
}

pub(crate) struct AgentStatusRow {
    pub name: String,
    #[allow(dead_code)] // informational; kept for diagnostics/logging
    pub provider: ProviderKind,
    pub status: AgentRowStatus,
}

pub(crate) struct BlockStatusRow {
    pub block_id: u32,
    pub source_block_id: u32,
    #[allow(dead_code)] // informational; kept for diagnostics/logging
    pub replica_index: u32,
    pub label: String,
    #[allow(dead_code)] // informational; kept for diagnostics/logging
    pub agent_name: String,
    #[allow(dead_code)] // informational; kept for diagnostics/logging
    pub provider: ProviderKind,
    pub status: AgentRowStatus,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(crate) enum StreamTarget {
    Agent(String),
    Block(u32),
}

pub(crate) struct StreamBuffer {
    text: String,
    max_bytes: usize,
}

impl StreamBuffer {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            text: String::new(),
            max_bytes,
        }
    }
    pub fn push(&mut self, chunk: &str) {
        self.text.push_str(chunk);
        if self.text.len() > self.max_bytes {
            let trim_to = self.text.len() - self.max_bytes;
            let mut start = trim_to;
            while start < self.text.len() && !self.text.is_char_boundary(start) {
                start += 1;
            }
            self.text.drain(..start);
        }
    }
    pub fn text(&self) -> &str {
        &self.text
    }
}

pub(crate) struct SetupAnalysisState {
    pub active: bool,
    pub loading: bool,
    pub content: String,
    pub scroll: u16,
    pub rx: Option<mpsc::UnboundedReceiver<Result<String, String>>>,
}

impl SetupAnalysisState {
    pub fn new() -> Self {
        Self {
            active: false,
            loading: false,
            content: String::new(),
            scroll: 0,
            rx: None,
        }
    }
    pub fn open_loading(&mut self) {
        self.active = true;
        self.loading = true;
        self.content.clear();
        self.scroll = 0;
    }
    /// Show a local message directly. Opens popup without LLM call.
    pub fn show_message(&mut self, msg: String) {
        self.active = true;
        self.loading = false;
        self.content = msg;
        self.scroll = 0;
    }
    pub fn close(&mut self) {
        self.active = false;
        self.loading = false;
        self.content.clear();
        self.scroll = 0;
        self.rx = None;
    }
}

pub(crate) struct HelpPopupState {
    pub active: bool,
    pub scroll: u16,
    pub tab: usize,
    pub tab_count: usize,
}

impl HelpPopupState {
    pub fn new() -> Self {
        Self {
            active: false,
            scroll: 0,
            tab: 0,
            tab_count: 1,
        }
    }
    pub fn open(&mut self, tab_count: usize) {
        self.active = true;
        self.scroll = 0;
        self.tab = 0;
        self.tab_count = tab_count.max(1);
    }
    pub fn close(&mut self) {
        self.active = false;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    Home,
    Prompt,
    Order,
    Running,
    Results,
    Pipeline,
    Memory,
}

pub struct App {
    pub(crate) config: AppConfig,
    pub(crate) config_path_override: Option<String>,
    /// Session overrides keyed by agent name
    pub(crate) session_overrides: HashMap<String, AgentConfig>,
    pub(crate) session_http_timeout_seconds: Option<u64>,
    pub(crate) session_model_fetch_timeout_seconds: Option<u64>,
    pub(crate) session_cli_timeout_seconds: Option<u64>,
    // Session-level memory config overrides (TUI only).
    // headless.rs reads config.memory.* directly because it has no session
    // override mechanism — that's intentional, not an oversight.
    // Note: db_path and project_id are intentionally excluded — they are
    // structural path fields resolved once at startup and not meaningful to
    // change mid-session via the config popup.
    pub(crate) session_memory_enabled: Option<bool>,
    pub(crate) session_memory_max_recall: Option<usize>,
    pub(crate) session_memory_max_recall_bytes: Option<usize>,
    pub(crate) session_memory_observation_ttl_days: Option<u32>,
    pub(crate) session_memory_summary_ttl_days: Option<u32>,
    pub(crate) session_memory_extraction_agent: Option<String>,
    pub(crate) session_memory_disable_extraction: Option<bool>,
    pub(crate) session_memory_stale_permanent_days: Option<u32>,
    pub(crate) screen: Screen,
    pub(crate) should_quit: bool,

    // Home screen state
    pub(crate) selected_agents: Vec<String>,
    pub(crate) selected_mode: ExecutionMode,
    pub(crate) home_cursor: usize,
    pub(crate) home_section: HomeSection,

    // Prompt / order / execution / results / edit / pipeline
    pub(crate) prompt: PromptState,
    pub(crate) order_cursor: usize,
    pub(crate) order_grabbed: Option<usize>,
    pub(crate) running: RunningState,
    pub(crate) results: ResultsState,
    pub(crate) edit_popup: EditPopupState,
    pub(crate) pipeline: PipelineState,

    // CLI availability per provider kind
    pub(crate) cli_available: HashMap<ProviderKind, bool>,

    // Help popup
    pub(crate) help_popup: HelpPopupState,

    // Setup analysis popup
    pub(crate) setup_analysis: SetupAnalysisState,

    // Memory
    pub(crate) memory: MemoryState,

    // Error modal
    pub(crate) error_modal: Option<String>,

    // Info modal (success messages)
    pub(crate) info_modal: Option<String>,
}

#[derive(Default)]
pub(crate) struct MemoryState {
    pub store: Option<crate::memory::store::MemoryStore>,
    pub project_id: String,
    pub last_recalled_count: usize,
    pub extraction_rx:
        Vec<mpsc::UnboundedReceiver<Result<Vec<crate::memory::types::ExtractedMemory>, String>>>,
    pub last_extraction_count: Option<usize>,
    pub last_extraction_error: Option<String>,
    /// When true, the management screen shows archived memories instead of active.
    pub management_show_archived: bool,
    // Management screen state
    pub management_memories: Vec<crate::memory::types::Memory>,
    pub management_cursor: usize,
    pub management_kind_filter: Option<crate::memory::types::MemoryKind>,
    /// When true, the list only shows memories with recall_count == 0.
    pub management_never_recalled_filter: bool,
    /// Total matching memories (ignoring LIMIT 500), for the header display.
    pub management_total_count: u64,
    /// Cached DB size in bytes, updated on memory screen entry / refresh.
    pub cached_db_size: Option<u64>,
    /// Two-step bulk delete: first D sets this, second D executes.
    pub pending_bulk_delete: bool,
}

pub(crate) struct PromptState {
    pub(crate) prompt_text: String,
    pub(crate) prompt_cursor: usize,
    pub(crate) session_name: String,
    pub(crate) iterations: u32,
    pub(crate) iterations_buf: String,
    pub(crate) runs: u32,
    pub(crate) runs_buf: String,
    pub(crate) concurrency: u32,
    pub(crate) concurrency_buf: String,
    pub(crate) resume_previous: bool,
    pub(crate) forward_prompt: bool,
    pub(crate) keep_session: bool,
    pub(crate) prompt_focus: PromptFocus,
}

pub(crate) struct RunningState {
    activity_log: VecDeque<ProgressEvent>,
    error_ledger: VecDeque<String>,
    recent_activity_logs: VecDeque<(String, String)>,
    last_error: Option<(String, String)>,
    completed_steps: usize,
    completed_agent_steps: HashSet<(String, u32)>,
    completed_block_steps: HashSet<(u32, u32, u32)>,
    pub(crate) expected_total_steps: usize,
    active_agents: HashSet<String>,
    active_blocks: Vec<(u32, String)>,
    pub(crate) is_running: bool,
    pub(crate) run_error: Option<String>,
    pub(crate) consolidation_active: bool,
    pub(crate) consolidation_phase: ConsolidationPhase,
    pub(crate) consolidation_target: ConsolidationTarget,
    pub(crate) consolidation_provider_cursor: usize,
    pub(crate) consolidation_prompt: String,
    pub(crate) consolidation_running: bool,
    pub(crate) consolidation_rx: Option<mpsc::UnboundedReceiver<Result<String, String>>>,
    pub(crate) diagnostic_running: bool,
    pub(crate) diagnostic_rx: Option<mpsc::UnboundedReceiver<Result<String, String>>>,
    pub(crate) batch_stage1_done: bool,
    pub(crate) multi_run_total: u32,
    pub(crate) multi_run_concurrency: u32,
    pub(crate) multi_run_cursor: usize,
    pub(crate) multi_run_states: Vec<RunState>,
    pub(crate) multi_run_step_labels: Vec<String>,
    pub(crate) progress_rx: Option<mpsc::UnboundedReceiver<ProgressEvent>>,
    pub(crate) batch_progress_rx: Option<mpsc::UnboundedReceiver<BatchProgressEvent>>,
    pub(crate) cancel_flag: Arc<AtomicBool>,
    pub(crate) run_dir: Option<PathBuf>,
    pub(crate) pending_single_execution: Option<PendingSingleExecution>,
    pub(crate) resume_prepare_rx:
        Option<mpsc::UnboundedReceiver<Result<ResumePreparation, String>>>,
    // Phase 1: Timing
    pub(crate) run_started_at: Option<Instant>,
    pub(crate) current_iteration: u32,
    pub(crate) final_iteration: u32,
    pub(crate) agent_timers: HashMap<String, AgentTimer>,
    pub(crate) block_timers: HashMap<u32, AgentTimer>,
    // Phase 2: Status rows
    pub(crate) agent_rows: Vec<AgentStatusRow>,
    pub(crate) block_rows: Vec<BlockStatusRow>,
    // Phase 4: Streaming
    pub(crate) stream_buffers: HashMap<StreamTarget, StreamBuffer>,
    pub(crate) preview_target: Option<StreamTarget>,
    pub(crate) show_activity_log: bool,
    pub(crate) batch_stage: Option<String>,
    pub(crate) batch_stage_error: Option<String>,
}

pub(crate) struct ResultsState {
    pub(crate) result_files: Vec<PathBuf>,
    pub(crate) result_cursor: usize,
    pub(crate) result_preview: String,
    pub(crate) batch_result_runs: Vec<BatchRunGroup>,
    pub(crate) batch_result_root_files: Vec<PathBuf>,
    pub(crate) batch_result_expanded: HashSet<u32>,
    pub(crate) batch_result_finalization_files: Vec<PathBuf>,
    pub(crate) batch_result_finalization_expanded: bool,
    pub(crate) results_loading: bool,
    pub(crate) results_load_rx: Option<mpsc::UnboundedReceiver<Result<ResultsLoadPayload, String>>>,
    pub(crate) preview_loading: bool,
    pub(crate) preview_request_id: u64,
    pub(crate) preview_rx: Option<mpsc::UnboundedReceiver<PreviewLoadResult>>,
}

pub(crate) struct EditPopupState {
    pub(crate) visible: bool,
    pub(crate) section: EditPopupSection,
    pub(crate) cursor: usize,
    pub(crate) timeout_cursor: usize,
    pub(crate) memory_cursor: usize,
    pub(crate) field: EditField,
    pub(crate) editing: bool,
    pub(crate) edit_buffer: String,
    pub(crate) model_picker_active: bool,
    pub(crate) model_picker_loading: bool,
    pub(crate) model_picker_list: Vec<String>,
    pub(crate) model_picker_filter: String,
    pub(crate) model_picker_cursor: usize,
    pub(crate) model_picker_rx: Option<mpsc::UnboundedReceiver<Result<Vec<String>, String>>>,
    pub(crate) config_save_in_progress: bool,
    pub(crate) config_save_rx: Option<mpsc::UnboundedReceiver<Result<AppConfig, String>>>,
}

pub(crate) struct PipelineState {
    pub(crate) pipeline_def: PipelineDefinition,
    pub(crate) pipeline_next_id: BlockId,
    pub(crate) pipeline_block_cursor: Option<BlockId>,
    pub(crate) pipeline_focus: PipelineFocus,
    pub(crate) pipeline_canvas_offset: (i16, i16),
    pub(crate) pipeline_prompt_cursor: usize,
    pub(crate) pipeline_session_name: String,
    pub(crate) pipeline_iterations_buf: String,
    pub(crate) pipeline_runs: u32,
    pub(crate) pipeline_runs_buf: String,
    pub(crate) pipeline_concurrency: u32,
    pub(crate) pipeline_concurrency_buf: String,
    pub(crate) pipeline_connecting_from: Option<BlockId>,
    pub(crate) pipeline_removing_conn: bool,
    pub(crate) pipeline_conn_cursor: usize,
    pub(crate) pipeline_show_edit: bool,
    pub(crate) pipeline_edit_field: PipelineEditField,
    pub(crate) pipeline_edit_name_buf: String,
    pub(crate) pipeline_edit_name_cursor: usize,
    pub(crate) pipeline_edit_agent_selection: Vec<bool>,
    pub(crate) pipeline_edit_agent_cursor: usize,
    pub(crate) pipeline_edit_agent_scroll: usize,
    pub(crate) pipeline_edit_agent_visible: Cell<usize>,
    pub(crate) pipeline_edit_profile_selection: Vec<bool>,
    pub(crate) pipeline_edit_profile_cursor: usize,
    pub(crate) pipeline_edit_profile_scroll: usize,
    pub(crate) pipeline_edit_profile_visible: Cell<usize>,
    pub(crate) pipeline_edit_profile_list: Vec<String>,
    /// Names of profiles assigned to the block but missing on disk — rendering marker for `[!]`.
    pub(crate) pipeline_edit_profile_orphaned: Vec<String>,
    /// Original profile order from block — used to preserve order on save.
    pub(crate) pipeline_edit_profile_original_order: Vec<String>,
    pub(crate) pipeline_edit_prompt_buf: String,
    pub(crate) pipeline_edit_prompt_cursor: usize,
    pub(crate) pipeline_edit_session_buf: String,
    pub(crate) pipeline_edit_session_cursor: usize,
    pub(crate) pipeline_edit_replicas_buf: String,
    pub(crate) pipeline_file_dialog: Option<PipelineDialogMode>,
    pub(crate) pipeline_file_input: String,
    pub(crate) pipeline_file_list: Vec<String>,
    pub(crate) pipeline_file_cursor: usize,
    pub(crate) pipeline_file_search: String,
    pub(crate) pipeline_file_search_focus: bool,
    pub(crate) pipeline_file_filtered: Vec<usize>,
    pub(crate) pipeline_file_scroll: usize,
    pub(crate) pipeline_file_visible: Cell<usize>,
    pub(crate) pipeline_save_path: Option<PathBuf>,
    pub(crate) pipeline_show_session_config: bool,
    pub(crate) pipeline_session_config_cursor: usize,
    /// 0 = Iter column, 1 = Loop column
    pub(crate) pipeline_session_config_col: usize,
    pub(crate) pipeline_loop_connecting_from: Option<BlockId>,
    pub(crate) pipeline_show_loop_edit: bool,
    pub(crate) pipeline_loop_edit_field: PipelineLoopEditField,
    pub(crate) pipeline_loop_edit_target: Option<(BlockId, BlockId)>,
    pub(crate) pipeline_loop_edit_count_buf: String,
    pub(crate) pipeline_loop_edit_prompt_buf: String,
    pub(crate) pipeline_loop_edit_prompt_cursor: usize,
    pub(crate) pipeline_feed_connecting_from: Option<BlockId>,
    pub(crate) pipeline_show_feed_edit: bool,
    pub(crate) pipeline_feed_edit_target: Option<(BlockId, BlockId)>,
    pub(crate) pipeline_feed_edit_field: PipelineFeedEditField,
    pub(crate) pipeline_show_feed_list: bool,
    pub(crate) pipeline_feed_list_cursor: usize,
    pub(crate) pipeline_feed_list_target: Option<BlockId>,
}

pub(crate) struct PendingSingleExecution {
    pub(crate) config: AppConfig,
    pub(crate) client: Client,
    pub(crate) raw_prompt: String,
    pub(crate) session_name: Option<String>,
    pub(crate) prompt_context: PromptRuntimeContext,
    pub(crate) agent_names: Vec<String>,
    pub(crate) mode: ExecutionMode,
    pub(crate) forward_prompt: bool,
    pub(crate) keep_session: bool,
    pub(crate) iterations: u32,
    pub(crate) cli_timeout_secs: u64,
    /// IDs of memories injected during recall, to be marked as recalled
    /// once execution is confirmed to be running.
    pub(crate) recalled_ids: Vec<i64>,
}

pub(crate) struct ResumePreparation {
    pub(crate) run_dir: PathBuf,
    pub(crate) start_iteration: u32,
    pub(crate) relay_initial_last_output: Option<String>,
    pub(crate) swarm_initial_outputs: HashMap<String, String>,
}

pub(crate) struct ResultsLoadPayload {
    pub(crate) result_files: Vec<PathBuf>,
    pub(crate) batch_result_runs: Vec<BatchRunGroup>,
    pub(crate) batch_result_root_files: Vec<PathBuf>,
    pub(crate) batch_result_expanded: HashSet<u32>,
    pub(crate) batch_result_finalization_files: Vec<PathBuf>,
}

pub(crate) struct PreviewLoadResult {
    pub(crate) request_id: u64,
    pub(crate) preview: String,
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
    Runs,
    Concurrency,
    Resume,
    ForwardPrompt,
    KeepSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditField {
    ApiKey,
    Model,
    ExtraCliArgs,
    OutputDir,
    TimeoutSeconds,
    AgentName,
    MemoryValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditPopupSection {
    Providers,
    Timeouts,
    Memory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationPhase {
    Confirm,
    Provider,
    Prompt,
    CrossRunConfirm,
    CrossRunPrompt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationTarget {
    Single,
    PerRun,
    AcrossRuns,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineFocus {
    InitialPrompt,
    SessionName,
    Iterations,
    Runs,
    Concurrency,
    Builder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Queued,
    Running,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStepStatus {
    Queued,
    Pending,
    Running,
    Done,
    Error,
}

#[derive(Debug, Clone)]
pub(crate) struct RunStepState {
    pub(crate) label: String,
    pub(crate) status: RunStepStatus,
}

#[derive(Debug, Clone)]
pub(crate) struct RunState {
    pub(crate) run_id: u32,
    pub(crate) status: RunStatus,
    pub(crate) steps: Vec<RunStepState>,
    pub(crate) recent_logs: VecDeque<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchRunGroup {
    pub(crate) run_id: u32,
    pub(crate) files: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineEditField {
    Name,
    Agent,
    Profile,
    Prompt,
    SessionId,
    Replicas,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PipelineLoopEditField {
    Count,
    Prompt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PipelineFeedEditField {
    Collection,
    Granularity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineDialogMode {
    Save,
    Load,
}

impl App {
    pub fn new(config: AppConfig) -> Self {
        let cli_available = crate::runtime_support::detect_cli_availability();
        let session_overrides =
            crate::runtime_support::compute_session_overrides(&config.agents, &cli_available);

        let memory = if config.memory.enabled {
            let db_path = if config.memory.db_path.is_empty() {
                config.resolved_output_dir().join("memory.db")
            } else {
                std::path::PathBuf::from(&config.memory.db_path)
            };
            match crate::memory::store::MemoryStore::open(&db_path) {
                Ok(store) => {
                    if config.memory.stale_permanent_days > 0 {
                        let _ = store.archive_stale_permanent(config.memory.stale_permanent_days);
                    }
                    MemoryState {
                        store: Some(store),
                        project_id: crate::memory::project::detect_project_id(
                            &config.memory.project_id,
                        ),
                        ..Default::default()
                    }
                }
                Err(e) => {
                    eprintln!("Warning: memory store failed to open ({db_path:?}): {e}");
                    MemoryState::default()
                }
            }
        } else {
            MemoryState::default()
        };

        Self {
            config,
            config_path_override: None,
            session_overrides,
            session_http_timeout_seconds: None,
            session_model_fetch_timeout_seconds: None,
            session_cli_timeout_seconds: None,
            session_memory_enabled: None,
            session_memory_max_recall: None,
            session_memory_max_recall_bytes: None,
            session_memory_observation_ttl_days: None,
            session_memory_summary_ttl_days: None,
            session_memory_extraction_agent: None,
            session_memory_disable_extraction: None,
            session_memory_stale_permanent_days: None,
            screen: Screen::Home,
            should_quit: false,
            selected_agents: Vec::new(),
            selected_mode: ExecutionMode::Swarm,
            home_cursor: 0,
            home_section: HomeSection::Agents,
            prompt: PromptState::new(),
            order_cursor: 0,
            order_grabbed: None,
            running: RunningState::new(),
            results: ResultsState::new(),
            edit_popup: EditPopupState::new(),
            pipeline: PipelineState::new(),
            cli_available,
            help_popup: HelpPopupState::new(),
            setup_analysis: SetupAnalysisState::new(),
            memory,
            error_modal: None,
            info_modal: None,
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
                let cli_ok = self
                    .cli_available
                    .get(&config.provider)
                    .copied()
                    .unwrap_or(false);
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

    pub fn effective_memory_enabled(&self) -> bool {
        self.session_memory_enabled
            .unwrap_or(self.config.memory.enabled)
    }

    pub fn effective_memory_max_recall(&self) -> usize {
        self.session_memory_max_recall
            .unwrap_or(self.config.memory.max_recall)
    }

    pub fn effective_memory_max_recall_bytes(&self) -> usize {
        self.session_memory_max_recall_bytes
            .unwrap_or(self.config.memory.max_recall_bytes)
    }

    pub fn effective_memory_observation_ttl_days(&self) -> u32 {
        self.session_memory_observation_ttl_days
            .unwrap_or(self.config.memory.observation_ttl_days)
    }

    pub fn effective_memory_summary_ttl_days(&self) -> u32 {
        self.session_memory_summary_ttl_days
            .unwrap_or(self.config.memory.summary_ttl_days)
    }

    pub fn effective_memory_stale_permanent_days(&self) -> u32 {
        self.session_memory_stale_permanent_days
            .unwrap_or(self.config.memory.stale_permanent_days)
    }

    pub fn effective_memory_extraction_agent(&self) -> &str {
        match &self.session_memory_extraction_agent {
            Some(s) => s.as_str(),
            None => &self.config.memory.extraction_agent,
        }
    }

    /// Resolve the extraction agent through the full fallback chain:
    /// explicit config/session override → first participant → first configured agent.
    /// Mode-aware: for Pipeline mode, participants come from the DAG definition.
    pub fn resolved_extraction_agent(&self) -> Option<String> {
        let participants = if self.selected_mode == ExecutionMode::Pipeline {
            self.pipeline.pipeline_def.all_agent_names()
        } else {
            self.selected_agents.clone()
        };
        resolve_extraction_agent(
            self.effective_memory_extraction_agent(),
            &participants,
            &self.config.agents,
        )
    }

    pub fn effective_memory_disable_extraction(&self) -> bool {
        self.session_memory_disable_extraction
            .unwrap_or(self.config.memory.disable_extraction)
    }

    /// Return a MemoryConfig with all session overrides applied.
    /// Used by extraction and other runtime paths that need the full config.
    pub fn effective_memory_config(&self) -> crate::config::MemoryConfig {
        crate::config::MemoryConfig {
            enabled: self.effective_memory_enabled(),
            db_path: self.config.memory.db_path.clone(),
            project_id: self.config.memory.project_id.clone(),
            max_recall: self.effective_memory_max_recall(),
            max_recall_bytes: self.effective_memory_max_recall_bytes(),
            extraction_agent: self.effective_memory_extraction_agent().to_string(),
            disable_extraction: self.effective_memory_disable_extraction(),
            observation_ttl_days: self.effective_memory_observation_ttl_days(),
            summary_ttl_days: self.effective_memory_summary_ttl_days(),
            stale_permanent_days: self.effective_memory_stale_permanent_days(),
        }
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

    pub fn init_multi_run_state(&mut self, runs: u32, concurrency: u32, step_labels: Vec<String>) {
        self.running.multi_run_total = runs;
        self.running.multi_run_concurrency = concurrency;
        self.running.multi_run_cursor = 0;
        self.running.multi_run_states = (1..=runs)
            .map(|run_id| RunState::new(run_id, &step_labels))
            .collect();
        self.running.multi_run_step_labels = step_labels;
    }

    pub fn clear_run_activity(&mut self) {
        self.running.clear_activity();
    }

    pub fn reset_running_state(&mut self) {
        self.screen = Screen::Running;
        self.running.reset_for_run();
        self.results.reset_for_run();
    }

    /// Preserve the legacy reset semantics from the monolithic TUI refactor.
    /// This intentionally leaves `running.is_running`/`running.run_error` alone
    /// because the old `reset_to_home()` only cleared the fields listed below.
    pub fn reset_to_home(&mut self) {
        self.screen = Screen::Home;
        self.prompt.reset_to_home();
        self.selected_agents.clear();
        self.clear_run_activity();
        self.results.reset_to_home();
        self.running.reset_to_home();
        self.pipeline = PipelineState::new();
        self.help_popup = HelpPopupState::new();
        self.setup_analysis.close();
    }

    pub fn record_progress(&mut self, event: ProgressEvent) {
        self.running.reduce_progress_event(&event);
        if !matches!(
            event,
            ProgressEvent::AgentStreamChunk { .. } | ProgressEvent::BlockStreamChunk { .. }
        ) {
            if self.running.activity_log.len() == ACTIVITY_LOG_LIMIT {
                self.running.activity_log.pop_front();
            }
            self.running.activity_log.push_back(event);
        }
    }

    pub fn activity_log(&self) -> &VecDeque<ProgressEvent> {
        &self.running.activity_log
    }

    pub fn error_ledger(&self) -> &VecDeque<String> {
        &self.running.error_ledger
    }

    pub fn recent_activity_logs(&self) -> &VecDeque<(String, String)> {
        &self.running.recent_activity_logs
    }

    pub fn last_error(&self) -> Option<&(String, String)> {
        self.running.last_error.as_ref()
    }

    pub fn completed_steps(&self) -> usize {
        self.running.completed_steps
    }

    pub fn is_agent_active(&self, name: &str) -> bool {
        self.running.active_agents.contains(name)
    }

    pub fn active_block_labels(&self) -> impl Iterator<Item = &str> {
        self.running
            .active_blocks
            .iter()
            .map(|(_, label)| label.as_str())
    }

    pub fn run_elapsed(&self) -> Duration {
        self.running
            .run_started_at
            .map(|t| t.elapsed())
            .unwrap_or_default()
    }

    pub fn current_iteration(&self) -> u32 {
        self.running.current_iteration
    }

    pub fn final_iteration(&self) -> u32 {
        self.running.final_iteration
    }

    pub fn agent_timer(&self, name: &str) -> Option<&AgentTimer> {
        self.running.agent_timers.get(name)
    }

    pub fn agent_rows(&self) -> &[AgentStatusRow] {
        &self.running.agent_rows
    }

    pub fn block_rows(&self) -> &[BlockStatusRow] {
        &self.running.block_rows
    }

    pub fn stream_buffer(&self, target: &StreamTarget) -> Option<&StreamBuffer> {
        self.running.stream_buffers.get(target)
    }

    pub fn preview_target(&self) -> Option<&StreamTarget> {
        self.running.preview_target.as_ref()
    }

    pub fn show_activity_log(&self) -> bool {
        self.running.show_activity_log
    }
}

impl PromptState {
    fn new() -> Self {
        Self {
            prompt_text: String::new(),
            prompt_cursor: 0,
            session_name: String::new(),
            iterations: 1,
            iterations_buf: "1".into(),
            runs: 1,
            runs_buf: "1".into(),
            concurrency: 0,
            concurrency_buf: "0".into(),
            resume_previous: false,
            forward_prompt: false,
            keep_session: true,
            prompt_focus: PromptFocus::Text,
        }
    }

    fn reset_to_home(&mut self) {
        self.prompt_text.clear();
        self.prompt_cursor = 0;
        self.session_name.clear();
        self.iterations = 1;
        self.iterations_buf = "1".into();
        self.runs = 1;
        self.runs_buf = "1".into();
        self.concurrency = 0;
        self.concurrency_buf = "0".into();
        self.resume_previous = false;
        self.forward_prompt = false;
        self.keep_session = true;
        self.prompt_focus = PromptFocus::Text;
    }
}

impl RunningState {
    fn new() -> Self {
        Self {
            activity_log: VecDeque::new(),
            error_ledger: VecDeque::new(),
            recent_activity_logs: VecDeque::new(),
            last_error: None,
            completed_steps: 0,
            completed_agent_steps: HashSet::new(),
            completed_block_steps: HashSet::new(),
            expected_total_steps: 0,
            active_agents: HashSet::new(),
            active_blocks: Vec::new(),
            is_running: false,
            run_error: None,
            consolidation_active: false,
            consolidation_phase: ConsolidationPhase::Confirm,
            consolidation_target: ConsolidationTarget::Single,
            consolidation_provider_cursor: 0,
            consolidation_prompt: String::new(),
            consolidation_running: false,
            consolidation_rx: None,
            diagnostic_running: false,
            diagnostic_rx: None,
            batch_stage1_done: false,
            multi_run_total: 0,
            multi_run_concurrency: 0,
            multi_run_cursor: 0,
            multi_run_states: Vec::new(),
            multi_run_step_labels: Vec::new(),
            progress_rx: None,
            batch_progress_rx: None,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            run_dir: None,
            pending_single_execution: None,
            resume_prepare_rx: None,
            run_started_at: None,
            current_iteration: 0,
            final_iteration: 0,
            agent_timers: HashMap::new(),
            block_timers: HashMap::new(),
            agent_rows: Vec::new(),
            block_rows: Vec::new(),
            stream_buffers: HashMap::new(),
            preview_target: None,
            show_activity_log: true,
            batch_stage: None,
            batch_stage_error: None,
        }
    }

    fn clear_activity(&mut self) {
        self.activity_log.clear();
        self.error_ledger.clear();
        self.recent_activity_logs.clear();
        self.last_error = None;
        self.completed_steps = 0;
        self.completed_agent_steps.clear();
        self.completed_block_steps.clear();
        self.active_agents.clear();
        self.active_blocks.clear();
        self.agent_timers.clear();
        self.block_timers.clear();
        self.agent_rows.clear();
        self.block_rows.clear();
        self.stream_buffers.clear();
        self.preview_target = None;
    }

    fn reset_for_run(&mut self) {
        self.clear_activity();
        self.is_running = true;
        self.run_error = None;
        self.consolidation_active = false;
        self.consolidation_phase = ConsolidationPhase::Confirm;
        self.consolidation_target = ConsolidationTarget::Single;
        self.consolidation_provider_cursor = 0;
        self.consolidation_prompt.clear();
        self.consolidation_running = false;
        self.consolidation_rx = None;
        self.diagnostic_running = false;
        self.diagnostic_rx = None;
        self.batch_stage1_done = false;
        self.batch_progress_rx = None;
        self.multi_run_total = 0;
        self.multi_run_concurrency = 0;
        self.multi_run_cursor = 0;
        self.multi_run_states.clear();
        self.multi_run_step_labels.clear();
        self.progress_rx = None;
        self.pending_single_execution = None;
        self.resume_prepare_rx = None;
        self.run_started_at = Some(Instant::now());
        self.current_iteration = 0;
        self.final_iteration = 0;
        self.show_activity_log = true;
        self.batch_stage = None;
        self.batch_stage_error = None;
    }

    fn reset_to_home(&mut self) {
        self.consolidation_active = false;
        self.consolidation_phase = ConsolidationPhase::Confirm;
        self.consolidation_target = ConsolidationTarget::Single;
        self.consolidation_provider_cursor = 0;
        self.consolidation_prompt.clear();
        self.consolidation_running = false;
        self.consolidation_rx = None;
        self.diagnostic_running = false;
        self.diagnostic_rx = None;
        self.batch_stage1_done = false;
        self.batch_progress_rx = None;
        self.multi_run_total = 0;
        self.multi_run_concurrency = 0;
        self.multi_run_cursor = 0;
        self.multi_run_states.clear();
        self.multi_run_step_labels.clear();
        self.progress_rx = None;
        self.run_dir = None;
        self.cancel_flag = Arc::new(AtomicBool::new(false));
        self.pending_single_execution = None;
        self.resume_prepare_rx = None;
        self.current_iteration = 0;
        self.final_iteration = 0;
        self.show_activity_log = true;
    }

    fn reduce_progress_event(&mut self, event: &ProgressEvent) {
        match event {
            ProgressEvent::AgentStarted { agent, .. } => {
                self.active_agents.insert(agent.clone());
                self.agent_timers.insert(agent.clone(), AgentTimer::new());
                self.stream_buffers
                    .remove(&StreamTarget::Agent(agent.clone()));
                if let Some(row) = self.agent_rows.iter_mut().find(|r| &r.name == agent) {
                    row.status = AgentRowStatus::Running;
                }
            }
            ProgressEvent::AgentLog {
                agent,
                iteration,
                message,
                ..
            } => {
                self.push_recent_activity_log(
                    agent.clone(),
                    format!("[iter {iteration}] {message}"),
                );
                // (log message used for activity log only, not stored on row)
            }
            ProgressEvent::AgentFinished {
                agent, iteration, ..
            } => {
                self.active_agents.remove(agent);
                if self
                    .completed_agent_steps
                    .insert((agent.clone(), *iteration))
                {
                    self.completed_steps += 1;
                }
                if let Some(t) = self.agent_timers.get_mut(agent) {
                    t.finish();
                }
                if let Some(row) = self.agent_rows.iter_mut().find(|r| r.name == *agent) {
                    row.status = AgentRowStatus::Finished;
                }
            }
            ProgressEvent::AgentError {
                agent,
                iteration,
                error,
                details,
                ..
            } => {
                self.active_agents.remove(agent);
                if self
                    .completed_agent_steps
                    .insert((agent.clone(), *iteration))
                {
                    self.completed_steps += 1;
                }
                let body = details.as_deref().unwrap_or(error);
                self.push_error_ledger_entry(format!("[{agent} iter {iteration}] {body}"));
                if let Some(details) = details {
                    self.last_error = Some((agent.clone(), details.clone()));
                }
                if let Some(t) = self.agent_timers.get_mut(agent) {
                    t.finish();
                }
                if let Some(row) = self.agent_rows.iter_mut().find(|r| r.name == *agent) {
                    row.status = AgentRowStatus::Error(crate::execution::truncate_chars(error, 60));
                }
            }
            ProgressEvent::IterationComplete { iteration } => {
                self.current_iteration = (*iteration + 1).min(self.final_iteration);
            }
            ProgressEvent::BlockStarted {
                block_id, label, ..
            } => {
                self.upsert_active_block(*block_id, label.clone());
                self.block_timers.insert(*block_id, AgentTimer::new());
                self.stream_buffers.remove(&StreamTarget::Block(*block_id));
                if let Some(row) = self.block_rows.iter_mut().find(|r| r.block_id == *block_id) {
                    row.status = AgentRowStatus::Running;
                }
            }
            ProgressEvent::BlockLog {
                agent_name,
                iteration,
                message,
                ..
            } => {
                self.push_recent_activity_log(
                    agent_name.clone(),
                    format!("[iter {iteration}] {message}"),
                );
                // (log message used for activity log only, not stored on row)
            }
            ProgressEvent::BlockFinished {
                block_id,
                iteration,
                loop_pass,
                ..
            } => {
                self.remove_active_block(*block_id);
                if self
                    .completed_block_steps
                    .insert((*block_id, *iteration, *loop_pass))
                {
                    self.completed_steps += 1;
                }
                if let Some(t) = self.block_timers.get_mut(block_id) {
                    t.finish();
                }
                if let Some(row) = self.block_rows.iter_mut().find(|r| r.block_id == *block_id) {
                    row.status = AgentRowStatus::Finished;
                }
            }
            ProgressEvent::BlockError {
                block_id,
                agent_name,
                iteration,
                loop_pass,
                error,
                details,
                label,
                ..
            } => {
                self.remove_active_block(*block_id);
                if self
                    .completed_block_steps
                    .insert((*block_id, *iteration, *loop_pass))
                {
                    self.completed_steps += 1;
                }
                let body = details.as_deref().unwrap_or(error);
                self.push_error_ledger_entry(format!(
                    "[{label} {agent_name} iter {iteration}] {body}"
                ));
                if let Some(details) = details {
                    self.last_error = Some((label.clone(), details.clone()));
                }
                if let Some(t) = self.block_timers.get_mut(block_id) {
                    t.finish();
                }
                if let Some(row) = self.block_rows.iter_mut().find(|r| r.block_id == *block_id) {
                    row.status = AgentRowStatus::Error(crate::execution::truncate_chars(error, 60));
                }
            }
            ProgressEvent::BlockSkipped {
                block_id,
                iteration,
                loop_pass,
                reason,
                ..
            } => {
                self.remove_active_block(*block_id);
                if self
                    .completed_block_steps
                    .insert((*block_id, *iteration, *loop_pass))
                {
                    self.completed_steps += 1;
                }
                if let Some(t) = self.block_timers.get_mut(block_id) {
                    t.finish();
                }
                if let Some(row) = self.block_rows.iter_mut().find(|r| r.block_id == *block_id) {
                    row.status =
                        AgentRowStatus::Skipped(crate::execution::truncate_chars(reason, 60));
                }
            }
            ProgressEvent::AgentStreamChunk { agent, chunk, .. } => {
                let target = StreamTarget::Agent(agent.clone());
                self.stream_buffers
                    .entry(target)
                    .or_insert_with(|| StreamBuffer::new(32 * 1024))
                    .push(chunk);
            }
            ProgressEvent::BlockStreamChunk {
                block_id, chunk, ..
            } => {
                let target = StreamTarget::Block(*block_id);
                self.stream_buffers
                    .entry(target)
                    .or_insert_with(|| StreamBuffer::new(32 * 1024))
                    .push(chunk);
            }
            ProgressEvent::AllDone => {}
        }
    }

    fn push_recent_activity_log(&mut self, name: String, message: String) {
        self.recent_activity_logs.push_back((name, message));
        if self.recent_activity_logs.len() > RECENT_ACTIVITY_LOG_LIMIT {
            self.recent_activity_logs.pop_front();
        }
    }

    fn push_error_ledger_entry(&mut self, entry: String) {
        let bounded = truncate_error_ledger_entry(&entry);
        self.error_ledger.push_back(bounded);
        if self.error_ledger.len() > ERROR_LEDGER_LIMIT {
            self.error_ledger.pop_front();
        }
    }

    fn upsert_active_block(&mut self, block_id: u32, label: String) {
        self.remove_active_block(block_id);
        self.active_blocks.push((block_id, label));
    }

    fn remove_active_block(&mut self, block_id: u32) {
        self.active_blocks.retain(|(id, _)| *id != block_id);
    }
}

impl ResultsState {
    fn new() -> Self {
        Self {
            result_files: Vec::new(),
            result_cursor: 0,
            result_preview: String::new(),
            batch_result_runs: Vec::new(),
            batch_result_root_files: Vec::new(),
            batch_result_expanded: HashSet::new(),
            batch_result_finalization_files: Vec::new(),
            batch_result_finalization_expanded: false,
            results_loading: false,
            results_load_rx: None,
            preview_loading: false,
            preview_request_id: 0,
            preview_rx: None,
        }
    }

    pub fn reset_to_home(&mut self) {
        self.result_files.clear();
        self.result_cursor = 0;
        self.result_preview.clear();
        self.batch_result_runs.clear();
        self.batch_result_root_files.clear();
        self.batch_result_expanded.clear();
        self.batch_result_finalization_files.clear();
        self.batch_result_finalization_expanded = false;
        self.results_loading = false;
        self.results_load_rx = None;
        self.preview_loading = false;
        self.preview_rx = None;
    }

    fn reset_for_run(&mut self) {
        self.reset_to_home();
    }
}

impl EditPopupState {
    fn new() -> Self {
        Self {
            visible: false,
            section: EditPopupSection::Providers,
            cursor: 0,
            timeout_cursor: 0,
            memory_cursor: 0,
            field: EditField::ApiKey,
            editing: false,
            edit_buffer: String::new(),
            model_picker_active: false,
            model_picker_loading: false,
            model_picker_list: Vec::new(),
            model_picker_filter: String::new(),
            model_picker_cursor: 0,
            model_picker_rx: None,
            config_save_in_progress: false,
            config_save_rx: None,
        }
    }
}

impl PipelineState {
    fn new() -> Self {
        Self {
            pipeline_def: PipelineDefinition::default(),
            pipeline_next_id: 1,
            pipeline_block_cursor: None,
            pipeline_focus: PipelineFocus::InitialPrompt,
            pipeline_canvas_offset: (0, 0),
            pipeline_prompt_cursor: 0,
            pipeline_session_name: String::new(),
            pipeline_iterations_buf: "1".into(),
            pipeline_runs: 1,
            pipeline_runs_buf: "1".into(),
            pipeline_concurrency: 0,
            pipeline_concurrency_buf: "0".into(),
            pipeline_connecting_from: None,
            pipeline_removing_conn: false,
            pipeline_conn_cursor: 0,
            pipeline_show_edit: false,
            pipeline_edit_field: PipelineEditField::Name,
            pipeline_edit_name_buf: String::new(),
            pipeline_edit_name_cursor: 0,
            pipeline_edit_agent_selection: Vec::new(),
            pipeline_edit_agent_cursor: 0,
            pipeline_edit_agent_scroll: 0,
            pipeline_edit_agent_visible: Cell::new(6),
            pipeline_edit_profile_selection: Vec::new(),
            pipeline_edit_profile_cursor: 0,
            pipeline_edit_profile_scroll: 0,
            pipeline_edit_profile_visible: Cell::new(4),
            pipeline_edit_profile_list: Vec::new(),
            pipeline_edit_profile_orphaned: Vec::new(),
            pipeline_edit_profile_original_order: Vec::new(),
            pipeline_edit_prompt_buf: String::new(),
            pipeline_edit_prompt_cursor: 0,
            pipeline_edit_session_buf: String::new(),
            pipeline_edit_session_cursor: 0,
            pipeline_edit_replicas_buf: "1".into(),
            pipeline_file_dialog: None,
            pipeline_file_input: String::new(),
            pipeline_file_list: Vec::new(),
            pipeline_file_cursor: 0,
            pipeline_file_search: String::new(),
            pipeline_file_search_focus: true,
            pipeline_file_filtered: Vec::new(),
            pipeline_file_scroll: 0,
            pipeline_file_visible: Cell::new(6),
            pipeline_save_path: None,
            pipeline_show_session_config: false,
            pipeline_session_config_cursor: 0,
            pipeline_session_config_col: 0,
            pipeline_loop_connecting_from: None,
            pipeline_show_loop_edit: false,
            pipeline_loop_edit_field: PipelineLoopEditField::Count,
            pipeline_loop_edit_target: None,
            pipeline_loop_edit_count_buf: String::new(),
            pipeline_loop_edit_prompt_buf: String::new(),
            pipeline_loop_edit_prompt_cursor: 0,
            pipeline_feed_connecting_from: None,
            pipeline_show_feed_edit: false,
            pipeline_feed_edit_target: None,
            pipeline_feed_edit_field: PipelineFeedEditField::Collection,
            pipeline_show_feed_list: false,
            pipeline_feed_list_cursor: 0,
            pipeline_feed_list_target: None,
        }
    }
}

fn truncate_error_ledger_entry(entry: &str) -> String {
    let clipped: String = entry.chars().take(ERROR_LEDGER_ENTRY_MAX_CHARS).collect();
    if entry.chars().count() > ERROR_LEDGER_ENTRY_MAX_CHARS {
        format!("{clipped}... [truncated]")
    } else {
        clipped
    }
}

impl RunState {
    pub fn new(run_id: u32, step_labels: &[String]) -> Self {
        Self {
            run_id,
            status: RunStatus::Queued,
            steps: step_labels
                .iter()
                .cloned()
                .map(|label| RunStepState {
                    label,
                    status: RunStepStatus::Queued,
                })
                .collect(),
            recent_logs: VecDeque::new(),
            error: None,
        }
    }

    pub fn push_log(&mut self, line: String) {
        self.recent_logs.push_back(line);
        while self.recent_logs.len() > RECENT_LOGS_CAP {
            self.recent_logs.pop_front();
        }
    }
}

/// Shared extraction-agent resolution: explicit config → first participant → first configured.
/// Used by TUI execution, headless, and the Home screen display.
pub(crate) fn resolve_extraction_agent(
    extraction_agent_config: &str,
    participants: &[String],
    configured_agents: &[crate::config::AgentConfig],
) -> Option<String> {
    let trimmed = extraction_agent_config.trim();
    if !trimmed.is_empty() {
        return Some(trimmed.to_string());
    }
    if let Some(first) = participants.first() {
        return Some(first.clone());
    }
    configured_agents.first().map(|a| a.name.clone())
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
            agent_cfg("OpenAI", ProviderKind::OpenAI, "k2", false),
            agent_cfg("Gemini", ProviderKind::Gemini, "", true),
        ];

        AppConfig {
            output_dir: "/tmp/out".to_string(),
            default_max_tokens: 4096,
            max_history_messages: 50,
            http_timeout_seconds: 120,
            model_fetch_timeout_seconds: 30,
            cli_timeout_seconds: 600,
            max_history_bytes: 102400,
            pipeline_block_concurrency: 0,
            diagnostic_provider: None,
            memory: crate::config::MemoryConfig::default(),
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
        assert_eq!(app.selected_mode, crate::execution::ExecutionMode::Swarm);
        assert_eq!(app.prompt.iterations, 1);
        assert_eq!(app.prompt.iterations_buf, "1");
        assert!(!app.running.is_running);
        assert_eq!(app.running.current_iteration, 0);
        assert_eq!(app.running.final_iteration, 0);
        assert!(app.running.show_activity_log);
    }

    #[test]
    fn reset_to_home_preserves_existing_semantics() {
        let mut app = App::new(base_config());
        app.selected_agents = vec!["Claude".into()];
        app.prompt.prompt_text = "prompt".into();
        app.prompt.prompt_cursor = 4;
        app.prompt.session_name = "session".into();
        app.prompt.iterations = 3;
        app.prompt.iterations_buf = "3".into();
        app.prompt.runs = 4;
        app.prompt.runs_buf = "4".into();
        app.prompt.concurrency = 2;
        app.prompt.concurrency_buf = "2".into();
        app.prompt.resume_previous = true;
        app.prompt.forward_prompt = true;
        app.running.is_running = false;
        app.running.run_error = Some("keep".into());
        app.running.progress_rx = Some(mpsc::unbounded_channel().1);
        app.running.batch_progress_rx = Some(mpsc::unbounded_channel().1);
        app.running.run_dir = Some(PathBuf::from("run"));
        app.running.cancel_flag = Arc::new(AtomicBool::new(true));
        app.running.consolidation_active = true;
        app.running.consolidation_phase = ConsolidationPhase::Prompt;
        app.running.consolidation_target = ConsolidationTarget::AcrossRuns;
        app.running.consolidation_provider_cursor = 2;
        app.running.consolidation_prompt = "extra".into();
        app.running.consolidation_running = true;
        app.running.consolidation_rx = Some(mpsc::unbounded_channel().1);
        app.running.diagnostic_running = true;
        app.running.diagnostic_rx = Some(mpsc::unbounded_channel().1);
        app.running.batch_stage1_done = true;
        app.running.multi_run_total = 3;
        app.running.multi_run_concurrency = 2;
        app.running.multi_run_cursor = 1;
        app.running.multi_run_states = vec![RunState::new(1, &["a".into()])];
        app.running.multi_run_step_labels = vec!["a".into()];
        app.results.result_files = vec![PathBuf::from("a.md")];
        app.results.result_preview = "preview".into();
        app.results.batch_result_runs = vec![BatchRunGroup {
            run_id: 1,
            files: vec![PathBuf::from("child.md")],
        }];
        app.results.batch_result_root_files = vec![PathBuf::from("root.md")];
        app.results.batch_result_expanded.insert(1);
        app.pipeline.pipeline_def.initial_prompt = "pipe".into();
        app.pipeline.pipeline_next_id = 5;
        app.pipeline.pipeline_block_cursor = Some(2);
        app.pipeline.pipeline_focus = PipelineFocus::Builder;
        app.pipeline.pipeline_canvas_offset = (4, 7);
        app.pipeline.pipeline_prompt_cursor = 9;
        app.pipeline.pipeline_session_name = "pipeline".into();
        app.pipeline.pipeline_iterations_buf = "5".into();
        app.pipeline.pipeline_runs = 6;
        app.pipeline.pipeline_runs_buf = "6".into();
        app.pipeline.pipeline_concurrency = 4;
        app.pipeline.pipeline_concurrency_buf = "4".into();
        app.pipeline.pipeline_connecting_from = Some(3);
        app.pipeline.pipeline_removing_conn = true;
        app.pipeline.pipeline_conn_cursor = 2;
        app.pipeline.pipeline_show_edit = true;
        app.pipeline.pipeline_edit_field = PipelineEditField::Prompt;
        app.pipeline.pipeline_edit_name_buf = "name".into();
        app.pipeline.pipeline_edit_name_cursor = 2;
        app.pipeline.pipeline_edit_agent_selection = vec![true, false];
        app.pipeline.pipeline_edit_agent_cursor = 1;
        app.pipeline.pipeline_edit_agent_scroll = 1;
        app.pipeline.pipeline_edit_profile_selection = vec![true, false, true];
        app.pipeline.pipeline_edit_profile_cursor = 2;
        app.pipeline.pipeline_edit_profile_scroll = 1;
        app.pipeline.pipeline_edit_profile_list = vec!["a".into(), "b".into(), "c".into()];
        app.pipeline.pipeline_edit_profile_orphaned = vec!["orphan".into()];
        app.pipeline.pipeline_edit_profile_original_order = vec!["c".into(), "a".into()];
        app.pipeline.pipeline_edit_prompt_buf = "prompt".into();
        app.pipeline.pipeline_edit_prompt_cursor = 3;
        app.pipeline.pipeline_edit_session_buf = "sid".into();
        app.pipeline.pipeline_edit_session_cursor = 2;
        app.pipeline.pipeline_file_dialog = Some(PipelineDialogMode::Save);
        app.pipeline.pipeline_file_input = "file".into();
        app.pipeline.pipeline_file_list = vec!["one".into()];
        app.pipeline.pipeline_file_cursor = 1;
        app.pipeline.pipeline_file_search = "query".into();
        app.pipeline.pipeline_file_search_focus = false;
        app.pipeline.pipeline_file_filtered = vec![0];
        app.pipeline.pipeline_save_path = Some(PathBuf::from("pipeline.toml"));
        app.pipeline.pipeline_show_session_config = true;
        app.pipeline.pipeline_session_config_cursor = 2;
        app.pipeline.pipeline_session_config_col = 1;
        app.help_popup.open(crate::screen::help::PIPELINE_TAB_COUNT);
        app.help_popup.tab = 3;
        app.help_popup.scroll = 15;
        app.setup_analysis.active = true;
        app.setup_analysis.content = "test".into();

        app.reset_to_home();

        assert_eq!(app.screen, Screen::Home);
        assert!(app.selected_agents.is_empty());
        assert_eq!(app.prompt.prompt_text, "");
        assert_eq!(app.prompt.prompt_cursor, 0);
        assert_eq!(app.prompt.session_name, "");
        assert_eq!(app.prompt.iterations, 1);
        assert_eq!(app.prompt.iterations_buf, "1");
        assert_eq!(app.prompt.runs, 1);
        assert_eq!(app.prompt.runs_buf, "1");
        assert_eq!(app.prompt.concurrency, 0);
        assert_eq!(app.prompt.concurrency_buf, "0");
        assert!(!app.prompt.resume_previous);
        assert!(!app.prompt.forward_prompt);
        assert!(app.results.result_files.is_empty());
        assert_eq!(app.results.result_preview, "");
        assert!(app.results.batch_result_runs.is_empty());
        assert!(app.results.batch_result_root_files.is_empty());
        assert!(app.results.batch_result_expanded.is_empty());
        assert!(!app.running.consolidation_active);
        assert_eq!(app.running.consolidation_phase, ConsolidationPhase::Confirm);
        assert_eq!(
            app.running.consolidation_target,
            ConsolidationTarget::Single
        );
        assert_eq!(app.running.consolidation_provider_cursor, 0);
        assert_eq!(app.running.consolidation_prompt, "");
        assert!(!app.running.consolidation_running);
        assert!(app.running.consolidation_rx.is_none());
        assert!(!app.running.diagnostic_running);
        assert!(app.running.diagnostic_rx.is_none());
        assert!(!app.running.batch_stage1_done);
        assert!(app.running.batch_progress_rx.is_none());
        assert_eq!(app.running.multi_run_total, 0);
        assert_eq!(app.running.multi_run_concurrency, 0);
        assert_eq!(app.running.multi_run_cursor, 0);
        assert!(app.running.multi_run_states.is_empty());
        assert!(app.running.multi_run_step_labels.is_empty());
        assert!(app.running.progress_rx.is_none());
        assert!(app.running.run_dir.is_none());
        assert!(!app
            .running
            .cancel_flag
            .load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(app.running.run_error.as_deref(), Some("keep"));
        assert!(!app.running.is_running);
        assert_eq!(app.running.current_iteration, 0);
        assert_eq!(app.running.final_iteration, 0);
        assert!(app.running.show_activity_log);
        assert_eq!(app.pipeline.pipeline_next_id, 1);
        assert!(app.pipeline.pipeline_def.blocks.is_empty());
        assert_eq!(app.pipeline.pipeline_def.initial_prompt, "");
        assert_eq!(app.pipeline.pipeline_focus, PipelineFocus::InitialPrompt);
        assert_eq!(app.pipeline.pipeline_canvas_offset, (0, 0));
        assert_eq!(app.pipeline.pipeline_prompt_cursor, 0);
        assert_eq!(app.pipeline.pipeline_session_name, "");
        assert_eq!(app.pipeline.pipeline_iterations_buf, "1");
        assert_eq!(app.pipeline.pipeline_runs, 1);
        assert_eq!(app.pipeline.pipeline_runs_buf, "1");
        assert_eq!(app.pipeline.pipeline_concurrency, 0);
        assert_eq!(app.pipeline.pipeline_concurrency_buf, "0");
        assert!(app.pipeline.pipeline_connecting_from.is_none());
        assert!(!app.pipeline.pipeline_removing_conn);
        assert_eq!(app.pipeline.pipeline_conn_cursor, 0);
        assert!(!app.pipeline.pipeline_show_edit);
        assert_eq!(app.pipeline.pipeline_edit_field, PipelineEditField::Name);
        assert_eq!(app.pipeline.pipeline_edit_name_buf, "");
        assert_eq!(app.pipeline.pipeline_edit_name_cursor, 0);
        assert!(app.pipeline.pipeline_edit_agent_selection.is_empty());
        assert_eq!(app.pipeline.pipeline_edit_agent_cursor, 0);
        assert_eq!(app.pipeline.pipeline_edit_agent_scroll, 0);
        assert!(app.pipeline.pipeline_edit_profile_selection.is_empty());
        assert_eq!(app.pipeline.pipeline_edit_profile_cursor, 0);
        assert_eq!(app.pipeline.pipeline_edit_profile_scroll, 0);
        assert!(app.pipeline.pipeline_edit_profile_list.is_empty());
        assert!(app.pipeline.pipeline_edit_profile_orphaned.is_empty());
        assert!(app.pipeline.pipeline_edit_profile_original_order.is_empty());
        assert_eq!(app.pipeline.pipeline_edit_prompt_buf, "");
        assert_eq!(app.pipeline.pipeline_edit_prompt_cursor, 0);
        assert_eq!(app.pipeline.pipeline_edit_session_buf, "");
        assert_eq!(app.pipeline.pipeline_edit_session_cursor, 0);
        assert!(app.pipeline.pipeline_file_dialog.is_none());
        assert_eq!(app.pipeline.pipeline_file_input, "");
        assert!(app.pipeline.pipeline_file_list.is_empty());
        assert_eq!(app.pipeline.pipeline_file_cursor, 0);
        assert!(app.pipeline.pipeline_file_search.is_empty());
        assert!(app.pipeline.pipeline_file_search_focus);
        assert!(app.pipeline.pipeline_file_filtered.is_empty());
        assert_eq!(app.pipeline.pipeline_file_scroll, 0);
        assert_eq!(app.pipeline.pipeline_file_visible.get(), 6);
        assert!(app.pipeline.pipeline_save_path.is_none());
        assert!(!app.pipeline.pipeline_show_session_config);
        assert_eq!(app.pipeline.pipeline_session_config_cursor, 0);
        assert_eq!(app.pipeline.pipeline_session_config_col, 0);
        assert!(!app.help_popup.active);
        assert_eq!(app.help_popup.tab, 0);
        assert_eq!(app.help_popup.scroll, 0);
        assert!(!app.setup_analysis.active);
        assert!(app.setup_analysis.content.is_empty());
    }

    #[test]
    fn record_progress_caps_activity_log_and_tracks_summary_state() {
        let mut app = App::new(base_config());
        for iteration in 0..(ACTIVITY_LOG_LIMIT as u32 + 5) {
            app.record_progress(ProgressEvent::AgentLog {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration,
                message: format!("m{iteration}"),
            });
        }

        assert_eq!(app.activity_log().len(), ACTIVITY_LOG_LIMIT);
        assert_eq!(
            app.recent_activity_logs().back(),
            Some(&(
                "Claude".to_string(),
                format!(
                    "[iter {}] m{}",
                    ACTIVITY_LOG_LIMIT as u32 + 4,
                    ACTIVITY_LOG_LIMIT as u32 + 4
                )
            ))
        );

        app.record_progress(ProgressEvent::AgentStarted {
            agent: "Claude".into(),
            kind: ProviderKind::Anthropic,
            iteration: 1,
        });
        assert!(app.is_agent_active("Claude"));

        app.record_progress(ProgressEvent::AgentError {
            agent: "Claude".into(),
            kind: ProviderKind::Anthropic,
            iteration: 1,
            error: "boom".into(),
            details: Some("full boom".into()),
        });
        assert_eq!(app.completed_steps(), 1);
        assert_eq!(
            app.error_ledger().iter().cloned().collect::<Vec<_>>(),
            vec!["[Claude iter 1] full boom".to_string()]
        );
        assert_eq!(
            app.last_error(),
            Some(&("Claude".to_string(), "full boom".to_string()))
        );
        assert!(!app.is_agent_active("Claude"));
    }

    #[test]
    fn error_ledger_is_bounded_by_size_and_count() {
        let mut app = App::new(base_config());
        let oversized = "x".repeat(ERROR_LEDGER_ENTRY_MAX_CHARS + 32);

        for iteration in 0..(ERROR_LEDGER_LIMIT as u32 + 5) {
            app.record_progress(ProgressEvent::AgentError {
                agent: "Claude".into(),
                kind: ProviderKind::Anthropic,
                iteration,
                error: "boom".into(),
                details: Some(oversized.clone()),
            });
        }

        assert_eq!(app.error_ledger().len(), ERROR_LEDGER_LIMIT);
        assert!(app.error_ledger()[0].contains("iter 5"));
        assert!(app
            .error_ledger()
            .back()
            .unwrap()
            .ends_with("... [truncated]"));
    }

    #[test]
    fn effective_agent_config_prefers_session_override() {
        let mut app = app_with_known_cli();
        app.session_overrides.insert(
            "OpenAI".to_string(),
            agent_cfg("OpenAI", ProviderKind::OpenAI, "override", true),
        );
        let cfg = app.effective_agent_config("OpenAI").expect("config");
        assert_eq!(cfg.api_key, "override");
        assert!(cfg.use_cli);
    }

    #[test]
    fn effective_agent_config_falls_back_to_global() {
        let app = app_with_known_cli();
        let cfg = app.effective_agent_config("Claude").expect("config");
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
    fn effective_memory_values_use_global_defaults() {
        let app = app_with_known_cli();
        assert!(app.effective_memory_enabled());
        assert_eq!(app.effective_memory_max_recall(), 20);
        assert_eq!(app.effective_memory_max_recall_bytes(), 16384);
        assert_eq!(app.effective_memory_observation_ttl_days(), 120);
        assert_eq!(app.effective_memory_summary_ttl_days(), 180);
        assert_eq!(app.effective_memory_extraction_agent(), "");
        assert!(!app.effective_memory_disable_extraction());
    }

    #[test]
    fn effective_memory_values_use_session_overrides() {
        let mut app = app_with_known_cli();
        app.session_memory_enabled = Some(false);
        app.session_memory_max_recall = Some(5);
        app.session_memory_max_recall_bytes = Some(2048);
        app.session_memory_observation_ttl_days = Some(30);
        app.session_memory_summary_ttl_days = Some(60);
        app.session_memory_extraction_agent = Some("Claude".into());
        app.session_memory_disable_extraction = Some(true);
        assert!(!app.effective_memory_enabled());
        assert_eq!(app.effective_memory_max_recall(), 5);
        assert_eq!(app.effective_memory_max_recall_bytes(), 2048);
        assert_eq!(app.effective_memory_observation_ttl_days(), 30);
        assert_eq!(app.effective_memory_summary_ttl_days(), 60);
        assert_eq!(app.effective_memory_extraction_agent(), "Claude");
        assert!(app.effective_memory_disable_extraction());
    }

    #[test]
    fn effective_memory_config_applies_all_overrides() {
        let mut app = app_with_known_cli();
        app.session_memory_max_recall = Some(3);
        app.session_memory_observation_ttl_days = Some(10);
        let cfg = app.effective_memory_config();
        assert_eq!(cfg.max_recall, 3);
        assert_eq!(cfg.observation_ttl_days, 10);
        // Non-overridden fields should come from config defaults
        assert_eq!(cfg.max_recall_bytes, 16384);
        assert_eq!(cfg.summary_ttl_days, 180);
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
        app.selected_agents = vec!["Claude".into(), "OpenAI".into(), "Gemini".into()];
        app.order_cursor = 2;
        app.move_order_up();
        assert_eq!(app.order_cursor, 1);
        assert_eq!(app.selected_agents, vec!["Claude", "OpenAI", "Gemini"]);
    }

    #[test]
    fn move_order_up_with_grabbed_swaps_agents() {
        let mut app = app_with_known_cli();
        app.selected_agents = vec!["Claude".into(), "OpenAI".into(), "Gemini".into()];
        app.order_cursor = 2;
        app.order_grabbed = Some(2);
        app.move_order_up();
        assert_eq!(app.selected_agents, vec!["Claude", "Gemini", "OpenAI"]);
        assert_eq!(app.order_cursor, 1);
        assert_eq!(app.order_grabbed, Some(1));
    }

    #[test]
    fn move_order_down_without_grabbed_only_moves_cursor() {
        let mut app = app_with_known_cli();
        app.selected_agents = vec!["Claude".into(), "OpenAI".into(), "Gemini".into()];
        app.order_cursor = 0;
        app.move_order_down();
        assert_eq!(app.order_cursor, 1);
        assert_eq!(app.selected_agents, vec!["Claude", "OpenAI", "Gemini"]);
    }

    #[test]
    fn move_order_down_with_grabbed_swaps_agents() {
        let mut app = app_with_known_cli();
        app.selected_agents = vec!["Claude".into(), "OpenAI".into(), "Gemini".into()];
        app.order_cursor = 0;
        app.order_grabbed = Some(0);
        app.move_order_down();
        assert_eq!(app.selected_agents, vec!["OpenAI", "Claude", "Gemini"]);
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
            "OpenAI".to_string(),
            agent_cfg("OpenAI", ProviderKind::OpenAI, "", true),
        );
        app.cli_available.insert(ProviderKind::OpenAI, false);
        let agents = app.available_agents();
        let openai = agents.iter().find(|(a, _)| a.name == "OpenAI").unwrap();
        assert!(!openai.1);
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

    #[test]
    fn help_popup_open_close() {
        let mut s = HelpPopupState::new();
        assert!(!s.active);
        s.open(6);
        assert!(s.active);
        assert_eq!(s.tab_count, 6);
        assert_eq!(s.tab, 0);
        assert_eq!(s.scroll, 0);
        s.close();
        assert!(!s.active);
    }

    #[test]
    fn help_popup_open_clamps_zero_tab_count() {
        let mut s = HelpPopupState::new();
        s.open(0);
        assert_eq!(s.tab_count, 1);
    }

    #[test]
    fn block_error_ledger_uses_display_label_not_runtime_id() {
        let mut app = App::new(base_config());
        app.record_progress(ProgressEvent::BlockError {
            block_id: 0,
            agent_name: "Claude".into(),
            label: "Writer (r1)".into(),
            iteration: 2,
            loop_pass: 0,
            error: "timeout".into(),
            details: Some("provider timed out".into()),
        });
        let entries: Vec<_> = app.error_ledger().iter().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert!(
            entries[0].contains("[Writer (r1) Claude iter 2]"),
            "expected display label in error ledger, got: {}",
            entries[0]
        );
        assert!(entries[0].contains("provider timed out"));
    }

    #[test]
    fn block_error_ledger_unnamed_block_no_agent_duplication() {
        let mut app = App::new(base_config());
        app.record_progress(ProgressEvent::BlockError {
            block_id: 0,
            agent_name: "Claude".into(),
            label: "Block 1".into(),
            iteration: 1,
            loop_pass: 0,
            error: "fail".into(),
            details: None,
        });
        let entries: Vec<_> = app.error_ledger().iter().cloned().collect();
        assert_eq!(entries.len(), 1);
        // Should be "[Block 1 Claude iter 1] fail" — agent appears exactly once
        assert_eq!(entries[0], "[Block 1 Claude iter 1] fail");
    }

    // find_executable tests moved to runtime_support::tests
}

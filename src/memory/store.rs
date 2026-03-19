use super::extraction::floor_char_boundary;
use super::types::{ExtractedMemory, Memory, MemoryKind, RecalledSet};
use crate::config::MemoryConfig;
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Sanitize a single segment (no hyphens) for FTS5.
/// Short segments (<5 chars) use exact quoted matching only.
/// Longer segments also add a prefix wildcard for broader recall.
fn escape_fts5_segment(seg: &str) -> Option<String> {
    let cleaned: String = seg
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if cleaned.is_empty() {
        None
    } else if cleaned.len() >= 5 {
        Some(format!("(\"{cleaned}\" OR {cleaned}*)"))
    } else {
        Some(format!("\"{cleaned}\""))
    }
}

/// Sanitize a term for use in an FTS5 query.
/// Hyphens are treated as token separators (matching how the `unicode61`
/// tokenizer indexes them), so "session-based" produces queries for both
/// "session" and "based" individually.
fn escape_fts5_term(term: &str) -> Option<String> {
    let parts: Vec<String> = term.split('-').filter_map(escape_fts5_segment).collect();
    if parts.is_empty() {
        None
    } else if parts.len() == 1 {
        Some(parts.into_iter().next().unwrap())
    } else {
        // Multiple segments from a hyphenated term — AND them together
        // so "session-based" matches content containing both tokens.
        Some(format!("({})", parts.join(" AND ")))
    }
}

// ---------------------------------------------------------------------------
// Schema creation and migrations
// ---------------------------------------------------------------------------

/// Create the full schema from scratch at the current version.
fn create_fresh_schema(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            content TEXT NOT NULL,
            reasoning TEXT NOT NULL DEFAULT '',
            source_run TEXT NOT NULL DEFAULT '',
            source_agent TEXT NOT NULL DEFAULT '',
            evidence_count INTEGER NOT NULL DEFAULT 1,
            tags TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            expires_at TEXT,
            updated_at TEXT NOT NULL,
            recall_count INTEGER NOT NULL DEFAULT 0,
            last_recalled_at TEXT,
            archived INTEGER NOT NULL DEFAULT 0
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
            content, reasoning, tags,
            content=memories, content_rowid=id,
            tokenize='porter unicode61'
        );

        {FTS_TRIGGERS_SQL}

        CREATE INDEX IF NOT EXISTS idx_memories_project ON memories(project_id);
        CREATE INDEX IF NOT EXISTS idx_memories_project_kind ON memories(project_id, kind);
        CREATE INDEX IF NOT EXISTS idx_memories_project_recall ON memories(project_id, recall_count);
        CREATE INDEX IF NOT EXISTS idx_memories_expires ON memories(expires_at) WHERE expires_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_memories_archived ON memories(project_id, archived);

        PRAGMA user_version = {SCHEMA_VERSION};",
    ))
    .map_err(|e| format!("Failed to create memory schema: {e}"))
}

/// Migration 0→1: Rebuild FTS5 with Porter stemming.
/// The `memories` data table is untouched; only the FTS virtual table
/// and its triggers are recreated.
///
/// DDL runs inside a transaction so a crash mid-migration leaves the DB
/// unchanged. `PRAGMA user_version` is set after commit because it writes
/// directly to the DB header and is not transaction-safe — if only the
/// PRAGMA fails the migration will harmlessly re-run on next open since
/// the DDL is idempotent.
fn migrate_0_to_1(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(&format!(
        "BEGIN;
         DROP TRIGGER IF EXISTS memories_ai;
         DROP TRIGGER IF EXISTS memories_au;
         DROP TRIGGER IF EXISTS memories_ad;
         DROP TABLE IF EXISTS memories_fts;

         CREATE VIRTUAL TABLE memories_fts USING fts5(
             content, reasoning, tags,
             content=memories, content_rowid=id,
             tokenize='porter unicode61'
         );

         {FTS_TRIGGERS_SQL}

         -- Repopulate FTS index from existing data
         INSERT INTO memories_fts(rowid, content, reasoning, tags)
             SELECT id, content, reasoning, tags FROM memories;
         COMMIT;",
    ))
    .map_err(|e| format!("Migration 0→1 (Porter stemming) failed: {e}"))?;
    conn.execute_batch("PRAGMA user_version = 1;")
        .map_err(|e| format!("Migration 0→1 PRAGMA failed: {e}"))
}

/// Migration 1→2: Add recall tracking columns.
///
/// `ALTER TABLE ADD COLUMN` is not idempotent — replaying it after a crash
/// between COMMIT and PRAGMA would fail with "duplicate column name".
/// We guard each ADD COLUMN by checking `PRAGMA table_info` first.
fn migrate_1_to_2(conn: &Connection) -> Result<(), String> {
    let has_column = |col: &str| -> bool {
        conn.prepare("PRAGMA table_info(memories)")
            .and_then(|mut stmt| {
                stmt.query_map([], |row| row.get::<_, String>(1))
                    .map(|rows| rows.flatten().any(|name| name == col))
            })
            .unwrap_or(false)
    };

    conn.execute_batch("BEGIN;").map_err(|e| e.to_string())?;
    if !has_column("recall_count") {
        conn.execute_batch(
            "ALTER TABLE memories ADD COLUMN recall_count INTEGER NOT NULL DEFAULT 0;",
        )
        .map_err(|e| format!("Migration 1→2 (recall_count) failed: {e}"))?;
    }
    if !has_column("last_recalled_at") {
        conn.execute_batch("ALTER TABLE memories ADD COLUMN last_recalled_at TEXT;")
            .map_err(|e| format!("Migration 1→2 (last_recalled_at) failed: {e}"))?;
    }
    conn.execute_batch("COMMIT;").map_err(|e| e.to_string())?;
    conn.execute_batch("PRAGMA user_version = 2;")
        .map_err(|e| format!("Migration 1→2 PRAGMA failed: {e}"))
}

/// Migration 2→3: Recreate FTS triggers with WHEN guard on UPDATE.
/// The guard skips FTS churn for metadata-only updates (recall_count, etc.).
fn migrate_2_to_3(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(&format!(
        "BEGIN;
         DROP TRIGGER IF EXISTS memories_ai;
         DROP TRIGGER IF EXISTS memories_au;
         DROP TRIGGER IF EXISTS memories_ad;
         {FTS_TRIGGERS_SQL}
         COMMIT;",
    ))
    .map_err(|e| format!("Migration 2→3 (trigger WHEN guard) failed: {e}"))?;
    conn.execute_batch("PRAGMA user_version = 3;")
        .map_err(|e| format!("Migration 2→3 PRAGMA failed: {e}"))
}

/// Migration 3→4: Add composite index on (project_id, recall_count) for
/// efficient "never recalled" filtering in the memory management screen.
fn migrate_3_to_4(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_memories_project_recall ON memories(project_id, recall_count);",
    )
    .map_err(|e| format!("Migration 3→4 (recall index) failed: {e}"))?;
    conn.execute_batch("PRAGMA user_version = 4;")
        .map_err(|e| format!("Migration 3→4 PRAGMA failed: {e}"))
}

/// Migration 4→5: Add partial index on `expires_at` to speed up
/// `cleanup_expired()`, which runs on startup and when entering the
/// Memory Management screen. Only rows with a non-NULL `expires_at`
/// (observations and summaries) are indexed, keeping the index small.
fn migrate_4_to_5(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_memories_expires \
         ON memories(expires_at) WHERE expires_at IS NOT NULL;",
    )
    .map_err(|e| format!("Migration 4→5 (expires index) failed: {e}"))?;
    conn.execute_batch("PRAGMA user_version = 5;")
        .map_err(|e| format!("Migration 4→5 PRAGMA failed: {e}"))
}

/// Migration 5→6: Add `archived` column for permanent memory staleness.
fn migrate_5_to_6(conn: &Connection) -> Result<(), String> {
    let has_column = |col: &str| -> bool {
        conn.prepare("PRAGMA table_info(memories)")
            .and_then(|mut stmt| {
                stmt.query_map([], |row| row.get::<_, String>(1))
                    .map(|rows| rows.flatten().any(|name| name == col))
            })
            .unwrap_or(false)
    };
    conn.execute_batch("BEGIN;").map_err(|e| e.to_string())?;
    if !has_column("archived") {
        conn.execute_batch("ALTER TABLE memories ADD COLUMN archived INTEGER NOT NULL DEFAULT 0;")
            .map_err(|e| format!("Migration 5→6 (archived column) failed: {e}"))?;
    }
    conn.execute_batch("COMMIT;").map_err(|e| e.to_string())?;
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_memories_archived ON memories(project_id, archived);",
    )
    .map_err(|e| format!("Migration 5→6 (archived index) failed: {e}"))?;
    conn.execute_batch("PRAGMA user_version = 6;")
        .map_err(|e| format!("Migration 5→6 PRAGMA failed: {e}"))
}

#[derive(Clone)]
pub struct MemoryStore {
    conn: Arc<Mutex<Connection>>,
    db_path: std::path::PathBuf,
}

/// Current schema version. Increment when adding migrations.
const SCHEMA_VERSION: i32 = 6;

/// SQL for FTS5 triggers — shared between fresh creation and migrations.
/// The UPDATE trigger guards on FTS-indexed column changes only, so that
/// metadata-only updates (recall_count, evidence_count, timestamps) skip
/// the unnecessary FTS delete+reinsert cycle.
const FTS_TRIGGERS_SQL: &str = "
    CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
        INSERT INTO memories_fts(rowid, content, reasoning, tags)
        VALUES (new.id, new.content, new.reasoning, new.tags);
    END;

    CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories
    WHEN old.content != new.content OR old.reasoning != new.reasoning OR old.tags != new.tags
    BEGIN
        INSERT INTO memories_fts(memories_fts, rowid, content, reasoning, tags)
        VALUES ('delete', old.id, old.content, old.reasoning, old.tags);
        INSERT INTO memories_fts(rowid, content, reasoning, tags)
        VALUES (new.id, new.content, new.reasoning, new.tags);
    END;

    CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
        INSERT INTO memories_fts(memories_fts, rowid, content, reasoning, tags)
        VALUES ('delete', old.id, old.content, old.reasoning, old.tags);
    END;
";

impl MemoryStore {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create memory DB directory: {e}"))?;
        }
        let conn = Connection::open(path).map_err(|e| format!("Failed to open memory DB: {e}"))?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| format!("Failed to set WAL mode: {e}"))?;

        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(|e| format!("Failed to read schema version: {e}"))?;

        if version == 0 {
            // Check if this is a pre-versioned DB (has memories table) or truly fresh.
            let table_exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='memories'",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(false);

            if table_exists {
                // Existing pre-versioned DB: run all migrations
                migrate_0_to_1(&conn)?;
                migrate_1_to_2(&conn)?;
                migrate_2_to_3(&conn)?;
                migrate_3_to_4(&conn)?;
                migrate_4_to_5(&conn)?;
                migrate_5_to_6(&conn)?;
            } else {
                // Fresh DB: create everything at current version
                create_fresh_schema(&conn)?;
            }
        } else if version < SCHEMA_VERSION {
            // Run only the migrations we haven't applied yet
            if version < 1 {
                migrate_0_to_1(&conn)?;
            }
            if version < 2 {
                migrate_1_to_2(&conn)?;
            }
            if version < 3 {
                migrate_2_to_3(&conn)?;
            }
            if version < 4 {
                migrate_3_to_4(&conn)?;
            }
            if version < 5 {
                migrate_4_to_5(&conn)?;
            }
            if version < 6 {
                migrate_5_to_6(&conn)?;
            }
        }
        // version >= SCHEMA_VERSION: already current, no-op

        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path: path.to_path_buf(),
        };
        let _ = store.cleanup_expired();
        Ok(store)
    }

    /// Insert a memory. Returns the new row id on success. For duplicate
    /// decisions returns -1 (skipped). For duplicate observations returns the
    /// existing id (TTL refreshed).
    pub fn insert(
        &self,
        project_id: &str,
        mem: &ExtractedMemory,
        source_run: &str,
        source_agent: &str,
        config: &MemoryConfig,
    ) -> Result<i64, String> {
        // Trim whitespace before validation and storage
        let trimmed_content = mem.content.trim();
        if trimmed_content.is_empty() {
            return Err("Empty memory content".into());
        }
        if trimmed_content.len() < 15 {
            return Err("Memory content too short to be useful".into());
        }

        // Truncate overly long content to prevent bloated entries
        let content = if trimmed_content.len() > 1024 {
            trimmed_content[..floor_char_boundary(trimmed_content, 1024)].to_string()
        } else {
            trimmed_content.to_string()
        };

        // Trim and truncate reasoning — should be a brief justification
        let trimmed_reasoning = mem.reasoning.trim();
        let reasoning = if trimmed_reasoning.len() > 512 {
            trimmed_reasoning[..floor_char_boundary(trimmed_reasoning, 512)].to_string()
        } else {
            trimmed_reasoning.to_string()
        };

        // Normalize tags: lowercase, deduplicate, strip empty, cap at 5
        let tags = {
            let mut seen = std::collections::HashSet::new();
            let normalized: Vec<String> = mem
                .tags
                .iter()
                .map(|t| t.trim().to_lowercase())
                .filter(|t| !t.is_empty() && seen.insert(t.clone()))
                .take(5)
                .collect();
            normalized.join(",")
        };

        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();
        let kind_str = mem.kind.as_str();

        // Cross-kind deduplication
        match mem.kind {
            MemoryKind::Principle => {
                if let Some(existing_id) =
                    self.find_similar_memory(&conn, project_id, &content, "principle", 0.6)?
                {
                    self.reinforce_principle_inner(&conn, existing_id, &now)?;
                    return Ok(existing_id);
                }
            }
            MemoryKind::Decision => {
                // Higher threshold for decisions: numeric parameters (e.g.,
                // "max 20 connections" vs "max 50 connections") change few
                // words but represent materially different choices.
                if let Some(existing_id) =
                    self.find_similar_memory(&conn, project_id, &content, "decision", 0.85)?
                {
                    // Auto-unarchive if the duplicate was archived, resetting
                    // updated_at and last_recalled_at so it won't be immediately
                    // re-archived on restart.
                    conn.execute(
                        "UPDATE memories SET archived = 0, updated_at = ?1, last_recalled_at = ?1 WHERE id = ?2 AND archived = 1",
                        rusqlite::params![now, existing_id],
                    )
                    .map_err(|e| format!("Failed to unarchive decision: {e}"))?;
                    return Ok(-1); // skip duplicate decision
                }
            }
            MemoryKind::Observation => {
                // Raised from 0.6 to 0.8: factual observations differing by a
                // numeric parameter (e.g., "rate limit 100/min" vs "500/min")
                // are materially different facts that must be preserved.
                if let Some(existing_id) =
                    self.find_similar_memory(&conn, project_id, &content, "observation", 0.8)?
                {
                    // Refresh both updated_at and expires_at to truly extend TTL
                    let new_expires = compute_expires_at(&now, MemoryKind::Observation, config);
                    self.refresh_observation(&conn, existing_id, &now, new_expires.as_deref())?;
                    return Ok(existing_id);
                }
            }
            MemoryKind::Summary => {} // no dedup — summaries are run-specific
        }

        let expires_at = compute_expires_at(&now, mem.kind, config);

        conn.execute(
            "INSERT INTO memories (project_id, kind, content, reasoning, source_run, source_agent, evidence_count, tags, created_at, expires_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8, ?9, ?8)",
            rusqlite::params![
                project_id, kind_str, content, reasoning, source_run, source_agent, tags, now, expires_at
            ],
        )
        .map_err(|e| format!("Failed to insert memory: {e}"))?;

        Ok(conn.last_insert_rowid())
    }

    pub fn recall(
        &self,
        project_id: &str,
        terms: &[String],
        max: usize,
        max_bytes: usize,
    ) -> Result<RecalledSet, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        if terms.is_empty() {
            return Ok(RecalledSet {
                memories: vec![],
                total_bytes: 0,
            });
        }

        // Build FTS5 query: OR-join terms (sanitized for FTS5 safety)
        let escaped: Vec<String> = terms.iter().filter_map(|t| escape_fts5_term(t)).collect();
        if escaped.is_empty() {
            return Ok(RecalledSet {
                memories: vec![],
                total_bytes: 0,
            });
        }
        let fts_query = escaped.join(" OR ");

        // BM25 returns negative scores (more negative = better match), so
        // multiplying by a >1 boost makes the score *more* negative (higher
        // priority). Subtracting 0.01*recall_count likewise makes frequently-
        // recalled memories sort earlier. The freshness penalty (positive
        // addition) makes stale temporary memories sink toward zero.
        // Kind-based boost makes higher-priority kinds sort first.
        // Freshness decay: temporary memories (observation/summary) lose
        // 0.001 rank per day since last update so stale entries gradually sink.
        let mut stmt = conn
            .prepare(
                "SELECT m.id, m.project_id, m.kind, m.content, m.reasoning, m.source_run,
                        m.source_agent, m.evidence_count, m.tags, m.created_at,
                        m.expires_at, m.updated_at, m.recall_count, m.last_recalled_at,
                        m.archived
                 FROM memories m
                 JOIN memories_fts ON memories_fts.rowid = m.id
                 WHERE memories_fts MATCH ?1 AND m.project_id = ?2
                   AND m.archived = 0
                   AND (m.expires_at IS NULL OR m.expires_at > ?4)
                 ORDER BY bm25(memories_fts) * CASE m.kind
                            WHEN 'principle'   THEN 1.5
                            WHEN 'decision'    THEN 1.3
                            WHEN 'observation' THEN 1.0
                            WHEN 'summary'     THEN 0.8
                            ELSE 1.0
                          END
                          - 0.01 * MIN(m.recall_count, 100)
                          + CASE WHEN m.kind IN ('observation', 'summary')
                                 THEN 0.001 * MAX(0, julianday('now') - julianday(m.updated_at))
                                 ELSE 0.0
                            END
                 LIMIT ?3",
            )
            .map_err(|e| format!("FTS query prepare failed: {e}"))?;

        let now = chrono::Utc::now().to_rfc3339();
        let rows = stmt
            .query_map(
                rusqlite::params![fts_query, project_id, max * 3, now],
                |row| {
                    let kind_str: String = row.get(2)?;
                    let kind = kind_str
                        .parse::<MemoryKind>()
                        .unwrap_or(MemoryKind::Observation);
                    Ok(Memory {
                        id: row.get(0)?,
                        project_id: row.get(1)?,
                        kind,
                        content: row.get(3)?,
                        reasoning: row.get(4)?,
                        source_run: row.get(5)?,
                        source_agent: row.get(6)?,
                        evidence_count: row.get(7)?,
                        tags: row.get(8)?,
                        created_at: row.get(9)?,
                        expires_at: row.get(10)?,
                        updated_at: row.get(11)?,
                        recall_count: row.get(12)?,
                        last_recalled_at: row.get(13)?,
                        archived: row.get::<_, i64>(14)? != 0,
                    })
                },
            )
            .map_err(|e| format!("FTS query failed: {e}"))?;

        let scored: Vec<Memory> = rows.flatten().collect();

        // Truncate to max count and max_bytes, skipping oversized entries
        // so that one large memory doesn't suppress all smaller ones.
        // Per-entry overhead accounts for formatting in recall::format_memory_entry:
        // [KIND] prefix (~12), reasoning prefix (~15), newlines (~4), XML escaping (~10%).
        const PER_ENTRY_OVERHEAD: usize = 40;
        let mut memories = Vec::new();
        let mut total_bytes = 0usize;
        for mem in scored {
            let raw = mem.content.len() + mem.reasoning.len();
            let mem_bytes = raw + raw / 10 + PER_ENTRY_OVERHEAD;
            if total_bytes + mem_bytes > max_bytes {
                continue; // skip this one, try smaller ones
            }
            total_bytes += mem_bytes;
            memories.push(mem);
            if memories.len() >= max {
                break;
            }
        }

        Ok(RecalledSet {
            memories,
            total_bytes,
        })
    }

    pub fn list(
        &self,
        project_id: &str,
        kind_filter: Option<MemoryKind>,
        never_recalled_only: bool,
        show_archived: bool,
    ) -> Result<Vec<Memory>, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();

        let mut clauses = vec![
            "project_id = ?1".to_string(),
            "(expires_at IS NULL OR expires_at > ?2)".to_string(),
        ];
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(project_id.to_string()), Box::new(now.clone())];
        if let Some(kind) = kind_filter {
            params.push(Box::new(kind.as_str().to_string()));
            clauses.push(format!("kind = ?{}", params.len()));
        }
        if never_recalled_only {
            clauses.push("recall_count = 0".to_string());
        }
        clauses.push(format!("archived = {}", if show_archived { 1 } else { 0 }));
        let sql = format!(
            "SELECT id, project_id, kind, content, reasoning, source_run, source_agent, \
             evidence_count, tags, created_at, expires_at, updated_at, recall_count, \
             last_recalled_at, archived FROM memories WHERE {} ORDER BY updated_at DESC LIMIT 500",
            clauses.join(" AND ")
        );

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("List query failed: {e}"))?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let kind_str: String = row.get(2)?;
                let kind = kind_str
                    .parse::<MemoryKind>()
                    .unwrap_or(MemoryKind::Observation);
                Ok(Memory {
                    id: row.get(0)?,
                    project_id: row.get(1)?,
                    kind,
                    content: row.get(3)?,
                    reasoning: row.get(4)?,
                    source_run: row.get(5)?,
                    source_agent: row.get(6)?,
                    evidence_count: row.get(7)?,
                    tags: row.get(8)?,
                    created_at: row.get(9)?,
                    expires_at: row.get(10)?,
                    updated_at: row.get(11)?,
                    recall_count: row.get(12)?,
                    last_recalled_at: row.get(13)?,
                    archived: row.get::<_, i64>(14)? != 0,
                })
            })
            .map_err(|e| format!("List query failed: {e}"))?;

        let memories: Vec<Memory> = rows.flatten().collect();
        Ok(memories)
    }

    pub fn delete(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        conn.execute("DELETE FROM memories WHERE id = ?1", rusqlite::params![id])
            .map_err(|e| format!("Failed to delete memory: {e}"))?;
        Ok(())
    }

    /// Batch-delete multiple memories. Automatically chunks to stay within
    /// SQLite's 999-variable limit.
    pub fn delete_batch(&self, ids: &[i64]) -> Result<usize, String> {
        if ids.is_empty() {
            return Ok(0);
        }
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let mut total = 0usize;
        for chunk in ids.chunks(900) {
            let placeholders: Vec<String> =
                (0..chunk.len()).map(|i| format!("?{}", i + 1)).collect();
            let sql = format!(
                "DELETE FROM memories WHERE id IN ({})",
                placeholders.join(", ")
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("delete_batch prepare failed: {e}"))?;
            let params: Vec<Box<dyn rusqlite::types::ToSql>> = chunk
                .iter()
                .map(|id| Box::new(*id) as Box<dyn rusqlite::types::ToSql>)
                .collect();
            let params_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            total += stmt
                .execute(params_refs.as_slice())
                .map_err(|e| format!("delete_batch failed: {e}"))?;
        }
        Ok(total)
    }

    /// Batch-update recall tracking for memories that were just injected into
    /// a prompt. Called after successful recall, before execution starts.
    /// Automatically chunks to stay within SQLite's 999-variable limit.
    ///
    /// **Semantic note:** `recall_count` tracks the number of *recall decisions*,
    /// not the number of times the memory was surfaced to an execution. In batch
    /// runs, the same recall decision is cloned into N children — we increment
    /// once (one decision), not N times (one per child). Resume runs replay
    /// previously recalled context without re-incrementing. This keeps the
    /// ranking signal proportional to how often the system *chose* this memory,
    /// not how many parallel copies consumed it.
    pub fn mark_recalled(&self, ids: &[i64]) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();
        // Chunk size 897: leaves room for ?1 (timestamp) within the 999 limit.
        for chunk in ids.chunks(897) {
            let placeholders: Vec<String> =
                (0..chunk.len()).map(|i| format!("?{}", i + 2)).collect();
            let sql = format!(
                "UPDATE memories SET recall_count = recall_count + 1, last_recalled_at = ?1 WHERE id IN ({})",
                placeholders.join(", ")
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("mark_recalled prepare failed: {e}"))?;
            let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
                Vec::with_capacity(chunk.len() + 1);
            params.push(Box::new(now.clone()));
            for id in chunk {
                params.push(Box::new(*id));
            }
            let params_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            stmt.execute(params_refs.as_slice())
                .map_err(|e| format!("mark_recalled failed: {e}"))?;
        }
        Ok(())
    }

    /// Count non-expired memories matching the given filters (no row limit).
    pub fn count(
        &self,
        project_id: &str,
        kind_filter: Option<MemoryKind>,
        never_recalled_only: bool,
        show_archived: bool,
    ) -> Result<u64, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();

        let mut clauses = vec![
            "project_id = ?1".to_string(),
            "(expires_at IS NULL OR expires_at > ?2)".to_string(),
        ];
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(project_id.to_string()), Box::new(now)];
        if let Some(kind) = kind_filter {
            params.push(Box::new(kind.as_str().to_string()));
            clauses.push(format!("kind = ?{}", params.len()));
        }
        if never_recalled_only {
            clauses.push("recall_count = 0".to_string());
        }
        clauses.push(format!("archived = {}", if show_archived { 1 } else { 0 }));
        let sql = format!(
            "SELECT COUNT(*) FROM memories WHERE {}",
            clauses.join(" AND ")
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("Count query failed: {e}"))?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let count: u64 = stmt
            .query_row(params_refs.as_slice(), |row| row.get(0))
            .map_err(|e| format!("Count query failed: {e}"))?;
        Ok(count)
    }

    /// Return the approximate on-disk size of the database in bytes,
    /// including the WAL sidecar file if present.
    pub fn db_size_bytes(&self) -> Option<u64> {
        let conn = self.conn.lock().ok()?;
        let page_count: u64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0)).ok()?;
        let page_size: u64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0)).ok()?;
        let main_size = page_count * page_size;
        // Include the WAL file size for accurate disk usage reporting.
        let wal_path = self.db_path.with_extension("db-wal");
        let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        Some(main_size + wal_size)
    }

    pub fn cleanup_expired(&self) -> Result<usize, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();
        let count = conn
            .execute(
                "DELETE FROM memories WHERE expires_at IS NOT NULL AND expires_at < ?1",
                rusqlite::params![now],
            )
            .map_err(|e| format!("Failed to cleanup expired memories: {e}"))?;
        Ok(count)
    }

    /// Archive permanent memories (decisions, principles) that are stale.
    /// Requires both `updated_at` and `last_recalled_at` (when present) to
    /// be older than the threshold, so any mutation that refreshes
    /// `updated_at` automatically prevents re-archival.
    pub fn archive_stale_permanent(&self, stale_days: u32) -> Result<usize, String> {
        if stale_days == 0 {
            return Ok(0);
        }
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let threshold =
            (chrono::Utc::now() - chrono::Duration::days(i64::from(stale_days))).to_rfc3339();
        let count = conn
            .execute(
                "UPDATE memories SET archived = 1
                 WHERE kind IN ('decision', 'principle')
                   AND archived = 0
                   AND updated_at < ?1
                   AND (last_recalled_at IS NULL OR last_recalled_at < ?1)",
                rusqlite::params![threshold],
            )
            .map_err(|e| format!("archive_stale_permanent failed: {e}"))?;
        Ok(count)
    }

    pub fn unarchive(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE memories SET archived = 0, updated_at = ?1, last_recalled_at = ?1 WHERE id = ?2",
            rusqlite::params![now, id],
        )
        .map_err(|e| format!("unarchive failed: {e}"))?;
        Ok(())
    }

    fn reinforce_principle_inner(
        &self,
        conn: &Connection,
        id: i64,
        now: &str,
    ) -> Result<(), String> {
        conn.execute(
            "UPDATE memories SET evidence_count = evidence_count + 1, updated_at = ?1, archived = 0 WHERE id = ?2",
            rusqlite::params![now, id],
        )
        .map_err(|e| format!("Failed to reinforce principle: {e}"))?;
        Ok(())
    }

    /// Find an existing memory of the given kind that is textually similar
    /// (Jaccard word overlap above the caller-specified threshold).
    /// Used for cross-kind deduplication.
    /// Excludes expired memories so that stale observations are not resurrected.
    fn find_similar_memory(
        &self,
        conn: &Connection,
        project_id: &str,
        content: &str,
        kind: &str,
        threshold: f64,
    ) -> Result<Option<i64>, String> {
        let words: Vec<&str> = content.split_whitespace().take(10).collect();
        if words.is_empty() {
            return Ok(None);
        }
        let escaped: Vec<String> = words.iter().filter_map(|w| escape_fts5_term(w)).collect();
        if escaped.is_empty() {
            return Ok(None);
        }
        let fts_query = escaped.join(" OR ");
        let now = chrono::Utc::now().to_rfc3339();

        let mut stmt = conn
            .prepare(
                "SELECT m.id, m.content
                 FROM memories m
                 JOIN memories_fts ON memories_fts.rowid = m.id
                 WHERE memories_fts MATCH ?1 AND m.project_id = ?2 AND m.kind = ?3
                   AND (m.expires_at IS NULL OR m.expires_at > ?4)
                 ORDER BY bm25(memories_fts)
                 LIMIT 5",
            )
            .map_err(|e| format!("Similarity search failed: {e}"))?;

        let rows = stmt
            .query_map(rusqlite::params![fts_query, project_id, kind, now], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| format!("Similarity query failed: {e}"))?;

        for row in rows.flatten() {
            let (id, existing_content) = row;
            if text_similarity(content, &existing_content) > threshold {
                return Ok(Some(id));
            }
        }

        Ok(None)
    }

    fn refresh_observation(
        &self,
        conn: &Connection,
        id: i64,
        now: &str,
        new_expires_at: Option<&str>,
    ) -> Result<(), String> {
        conn.execute(
            "UPDATE memories SET updated_at = ?1, expires_at = ?2 WHERE id = ?3",
            rusqlite::params![now, new_expires_at, id],
        )
        .map_err(|e| format!("Failed to refresh observation: {e}"))?;
        Ok(())
    }
}

fn compute_expires_at(now: &str, kind: MemoryKind, config: &MemoryConfig) -> Option<String> {
    if kind.is_permanent() {
        return None;
    }
    let days = match kind {
        MemoryKind::Observation => config.observation_ttl_days,
        MemoryKind::Summary => config.summary_ttl_days,
        _ => return None,
    };
    chrono::DateTime::parse_from_rfc3339(now)
        .ok()
        .map(|dt| (dt + chrono::Duration::days(i64::from(days))).to_rfc3339())
}

/// Simple word-overlap similarity (Jaccard-like), case-insensitive.
fn text_similarity(a: &str, b: &str) -> f64 {
    let words_a: std::collections::HashSet<String> = a
        .split_whitespace()
        .map(|w| {
            w.trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|w| !w.is_empty())
        .collect();
    let words_b: std::collections::HashSet<String> = b
        .split_whitespace()
        .map(|w| {
            w.trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|w| !w.is_empty())
        .collect();
    if words_a.is_empty() || words_b.is_empty() {
        return 0.0;
    }
    let intersection = words_a.intersection(&words_b).count();
    let union = words_a.union(&words_b).count();
    intersection as f64 / union as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config() -> MemoryConfig {
        MemoryConfig::default()
    }

    #[test]
    fn insert_and_list() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let mem = ExtractedMemory {
            kind: MemoryKind::Decision,
            content: "Use Rust for the backend".into(),
            reasoning: "Performance matters".into(),
            tags: vec!["arch".into()],
        };
        let id = store
            .insert("proj1", &mem, "run1", "Claude", &test_config())
            .unwrap();
        assert!(id > 0);

        let list = store.list("proj1", None, false, false).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].content, "Use Rust for the backend");
        assert_eq!(list[0].kind, MemoryKind::Decision);
    }

    #[test]
    fn ttl_cleanup() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let conn = store.conn.lock().unwrap();
        // Insert an already-expired observation
        let past = "2020-01-01T00:00:00+00:00";
        conn.execute(
            "INSERT INTO memories (project_id, kind, content, reasoning, tags, created_at, expires_at, updated_at)
             VALUES ('proj1', 'observation', 'old data', '', '', ?1, ?1, ?1)",
            rusqlite::params![past],
        )
        .unwrap();
        drop(conn);

        let count = store.cleanup_expired().unwrap();
        assert_eq!(count, 1);
        assert!(store.list("proj1", None, false, false).unwrap().is_empty());
    }

    #[test]
    fn principle_reinforcement() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let mem = ExtractedMemory {
            kind: MemoryKind::Principle,
            content: "Always validate user input at boundaries".into(),
            reasoning: "Security best practice".into(),
            tags: vec![],
        };
        store.insert("proj1", &mem, "run1", "Claude", &cfg).unwrap();

        // Insert similar principle — should reinforce
        let mem2 = ExtractedMemory {
            kind: MemoryKind::Principle,
            content: "Always validate user input at system boundaries".into(),
            reasoning: "Security".into(),
            tags: vec![],
        };
        store
            .insert("proj1", &mem2, "run2", "Claude", &cfg)
            .unwrap();

        let list = store
            .list("proj1", Some(MemoryKind::Principle), false, false)
            .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].evidence_count, 2);
    }

    #[test]
    fn fts_recall() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();

        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use PostgreSQL for data storage".into(),
                    reasoning: "ACID compliance".into(),
                    tags: vec!["database".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Observation,
                    content: "The API response time is under 100ms".into(),
                    reasoning: "Benchmark results".into(),
                    tags: vec!["perf".into()],
                },
                "run1",
                "OpenAI",
                &cfg,
            )
            .unwrap();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Summary,
                    content: "Completed the logging infrastructure".into(),
                    reasoning: "".into(),
                    tags: vec![],
                },
                "run1",
                "Gemini",
                &cfg,
            )
            .unwrap();

        let result = store
            .recall("proj1", &["PostgreSQL".into(), "database".into()], 10, 8192)
            .unwrap();
        assert!(!result.memories.is_empty());
        assert_eq!(
            result.memories[0].content,
            "Use PostgreSQL for data storage"
        );
    }

    #[test]
    fn delete_memory() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let id = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Observation,
                    content: "Temporary observation for deletion test".into(),
                    reasoning: "".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        store.delete(id).unwrap();
        assert!(store.list("proj1", None, false, false).unwrap().is_empty());
    }

    #[test]
    fn open_bad_path() {
        let result = MemoryStore::open(std::path::Path::new("/nonexistent/deeply/nested/test.db"));
        assert!(result.is_err());
    }

    #[test]
    fn text_similarity_identical() {
        assert!((text_similarity("hello world", "hello world") - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn text_similarity_partial() {
        let sim = text_similarity("hello world foo", "hello world bar");
        assert!(sim > 0.4);
        assert!(sim < 1.0);
    }

    #[test]
    fn text_similarity_none() {
        assert!(text_similarity("alpha beta", "gamma delta").abs() < f64::EPSILON);
    }

    // --- New tests for memory quality improvements ---

    #[test]
    fn insert_rejects_too_short() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let result = store.insert(
            "proj1",
            &ExtractedMemory {
                kind: MemoryKind::Decision,
                content: "short".into(),
                reasoning: "".into(),
                tags: vec![],
            },
            "run1",
            "Claude",
            &test_config(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too short"));
    }

    #[test]
    fn insert_truncates_long_content() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let long_content = "x".repeat(2000);
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: long_content,
                    reasoning: "".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &test_config(),
            )
            .unwrap();
        let list = store.list("proj1", None, false, false).unwrap();
        assert_eq!(list.len(), 1);
        assert!(list[0].content.len() <= 1024);
    }

    #[test]
    fn insert_normalizes_tags() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use PostgreSQL for the persistence layer".into(),
                    reasoning: "ACID compliance needed".into(),
                    tags: vec![
                        " Database ".into(),
                        "database".into(), // dup
                        "PERF".into(),
                        "".into(), // empty
                        "arch".into(),
                    ],
                },
                "run1",
                "Claude",
                &test_config(),
            )
            .unwrap();
        let list = store.list("proj1", None, false, false).unwrap();
        assert_eq!(list.len(), 1);
        // Tags should be: database,perf,arch (lowercase, deduped, empty stripped)
        let tags: Vec<&str> = list[0].tags.split(',').collect();
        assert!(tags.contains(&"database"));
        assert!(tags.contains(&"perf"));
        assert!(tags.contains(&"arch"));
        assert_eq!(tags.len(), 3); // no duplicate
    }

    #[test]
    fn decision_dedup_skips_duplicate() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use connection pooling with max 20 connections".into(),
                    reasoning: "Performance under load".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        // Insert near-identical decision (same wording, minor suffix) — should be skipped
        let id2 = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use connection pooling with max 20 connections always".into(),
                    reasoning: "Load issues".into(),
                    tags: vec![],
                },
                "run2",
                "Claude",
                &cfg,
            )
            .unwrap();
        assert_eq!(id2, -1); // skipped
        assert_eq!(
            store
                .list("proj1", Some(MemoryKind::Decision), false, false)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn decision_dedup_preserves_parameterized_variants() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use connection pooling with max 20 connections".into(),
                    reasoning: "Performance under load".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        // Same structure but different numeric parameter — must NOT be deduped
        let id2 = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use connection pooling with max 50 connections".into(),
                    reasoning: "Increased capacity needed".into(),
                    tags: vec![],
                },
                "run2",
                "Claude",
                &cfg,
            )
            .unwrap();
        assert!(
            id2 > 0,
            "Parameterized variant should be inserted, not deduped"
        );
        assert_eq!(
            store
                .list("proj1", Some(MemoryKind::Decision), false, false)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn observation_dedup_refreshes_timestamp() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Observation,
                    content: "The Gemini API returns 429 errors above 60 requests".into(),
                    reasoning: "Load testing".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        let before = store.list("proj1", None, false, false).unwrap();
        let before_updated = before[0].updated_at.clone();

        // Brief pause to get a different timestamp
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Insert similar observation — should refresh updated_at
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Observation,
                    content: "The Gemini API returns 429 errors above 60 requests per minute"
                        .into(),
                    reasoning: "Retesting".into(),
                    tags: vec![],
                },
                "run2",
                "Claude",
                &cfg,
            )
            .unwrap();
        let after = store.list("proj1", None, false, false).unwrap();
        assert_eq!(after.len(), 1); // no duplicate
        assert!(after[0].updated_at > before_updated);
        // expires_at should also be refreshed (not stuck at original value)
        let before_expires = before[0].expires_at.clone().unwrap();
        let after_expires = after[0].expires_at.clone().unwrap();
        assert!(after_expires > before_expires);
    }

    #[test]
    fn observation_dedup_preserves_parameterized_variants() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Observation,
                    content: "API rate limit is 100 requests per minute".into(),
                    reasoning: "Initial testing".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        // Same structure but different numeric parameter — must NOT be deduped
        let id2 = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Observation,
                    content: "API rate limit is 500 requests per minute".into(),
                    reasoning: "Updated after scaling".into(),
                    tags: vec![],
                },
                "run2",
                "Claude",
                &cfg,
            )
            .unwrap();
        assert!(
            id2 > 0,
            "Parameterized observation variant should be inserted, not deduped"
        );
        assert_eq!(
            store
                .list("proj1", Some(MemoryKind::Observation), false, false)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn mark_recalled_updates_count() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let id = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use PostgreSQL for data storage system".into(),
                    reasoning: "ACID compliance".into(),
                    tags: vec!["database".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        assert!(id > 0);

        store.mark_recalled(&[id]).unwrap();
        store.mark_recalled(&[id]).unwrap();

        let list = store.list("proj1", None, false, false).unwrap();
        assert_eq!(list[0].recall_count, 2);
        assert!(list[0].last_recalled_at.is_some());
    }

    #[test]
    fn schema_version_set_on_fresh_db() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let conn = store.conn.lock().unwrap();
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn porter_stemming_works() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use database connection pooling for performance".into(),
                    reasoning: "Connection overhead".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        // "databases" should match "database" via Porter stemming
        let result = store
            .recall("proj1", &["databases".into()], 10, 8192)
            .unwrap();
        assert!(!result.memories.is_empty());
    }

    /// Create a v0 database (pre-versioning) and verify all migrations run.
    #[test]
    fn migrate_v0_to_current() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        // Manually create a v0 schema: no user_version, old FTS (no Porter), no recall columns.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE memories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    content TEXT NOT NULL,
                    reasoning TEXT NOT NULL DEFAULT '',
                    source_run TEXT NOT NULL DEFAULT '',
                    source_agent TEXT NOT NULL DEFAULT '',
                    evidence_count INTEGER NOT NULL DEFAULT 1,
                    tags TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE VIRTUAL TABLE memories_fts USING fts5(
                    content, reasoning, tags,
                    content=memories, content_rowid=id,
                    tokenize='unicode61'
                );
                CREATE INDEX idx_memories_project ON memories(project_id);
                CREATE INDEX idx_memories_project_kind ON memories(project_id, kind);",
            )
            .unwrap();
            // Insert a row so we can verify data survives migration.
            conn.execute(
                "INSERT INTO memories (project_id, kind, content, reasoning, created_at, updated_at)
                 VALUES ('p', 'decision', 'Use pooling for database connections', 'perf', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')",
                [],
            )
            .unwrap();
        }

        // Open through MemoryStore — should auto-migrate.
        let store = MemoryStore::open(&path).unwrap();
        let conn = store.conn.lock().unwrap();

        // Version should be current.
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // recall_count column should exist with default 0.
        let rc: i64 = conn
            .query_row("SELECT recall_count FROM memories WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(rc, 0);

        // Data should survive.
        drop(conn);
        let list = store.list("p", None, false, false).unwrap();
        assert_eq!(list.len(), 1);
        assert!(list[0].content.contains("pooling"));
    }

    /// Open a v2 database and verify migration 2→3 runs cleanly.
    #[test]
    fn migrate_v2_to_current() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE memories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    content TEXT NOT NULL,
                    reasoning TEXT NOT NULL DEFAULT '',
                    source_run TEXT NOT NULL DEFAULT '',
                    source_agent TEXT NOT NULL DEFAULT '',
                    evidence_count INTEGER NOT NULL DEFAULT 1,
                    tags TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    updated_at TEXT NOT NULL,
                    recall_count INTEGER NOT NULL DEFAULT 0,
                    last_recalled_at TEXT
                );
                CREATE VIRTUAL TABLE memories_fts USING fts5(
                    content, reasoning, tags,
                    content=memories, content_rowid=id,
                    tokenize='porter unicode61'
                );
                -- Old trigger without WHEN guard (v2 schema)
                CREATE TRIGGER memories_ai AFTER INSERT ON memories BEGIN
                    INSERT INTO memories_fts(rowid, content, reasoning, tags)
                    VALUES (new.id, new.content, new.reasoning, new.tags);
                END;
                CREATE TRIGGER memories_au AFTER UPDATE ON memories BEGIN
                    INSERT INTO memories_fts(memories_fts, rowid, content, reasoning, tags)
                    VALUES ('delete', old.id, old.content, old.reasoning, old.tags);
                    INSERT INTO memories_fts(rowid, content, reasoning, tags)
                    VALUES (new.id, new.content, new.reasoning, new.tags);
                END;
                CREATE TRIGGER memories_ad AFTER DELETE ON memories BEGIN
                    INSERT INTO memories_fts(memories_fts, rowid, content, reasoning, tags)
                    VALUES ('delete', old.id, old.content, old.reasoning, old.tags);
                END;
                PRAGMA user_version = 2;",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO memories (project_id, kind, content, reasoning, created_at, updated_at)
                 VALUES ('p', 'principle', 'Always test migration paths end to end', 'reliability', '2025-06-01T00:00:00Z', '2025-06-01T00:00:00Z')",
                [],
            )
            .unwrap();
        }

        let store = MemoryStore::open(&path).unwrap();
        let conn = store.conn.lock().unwrap();
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // Trigger should have been recreated with WHEN guard.
        // Verify the memories_au trigger SQL contains "WHEN".
        let trigger_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='memories_au'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            trigger_sql.contains("WHEN"),
            "Trigger should have WHEN guard after migration 2→3"
        );

        // Verify the recall index exists after migration 3→4.
        let recall_idx: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='index' AND name='idx_memories_project_recall'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(recall_idx, "Recall index should exist after migration 3→4");

        drop(conn);
        let list = store.list("p", None, false, false).unwrap();
        assert_eq!(list.len(), 1);
        assert!(list[0].content.contains("migration"));
    }

    #[test]
    fn list_never_recalled_only() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let id1 = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "First decision that is recalled".into(),
                    reasoning: "r1".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        let _id2 = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Second decision never recalled".into(),
                    reasoning: "r2".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();

        // Mark first as recalled
        store.mark_recalled(&[id1]).unwrap();

        // Without filter: both visible
        let all = store.list("proj1", None, false, false).unwrap();
        assert_eq!(all.len(), 2);

        // With never_recalled_only: only the second
        let cold = store.list("proj1", None, true, false).unwrap();
        assert_eq!(cold.len(), 1);
        assert!(cold[0].content.contains("never recalled"));
    }

    #[test]
    fn delete_batch_removes_multiple() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let mut ids = Vec::new();
        // Use Summary kind — no dedup, so all 5 insertions succeed.
        for i in 0..5 {
            let id = store
                .insert(
                    "proj1",
                    &ExtractedMemory {
                        kind: MemoryKind::Summary,
                        content: format!("Batch test summary for run number {i}"),
                        reasoning: "r".into(),
                        tags: vec![],
                    },
                    &format!("run{i}"),
                    "Claude",
                    &cfg,
                )
                .unwrap();
            ids.push(id);
        }
        assert_eq!(store.list("proj1", None, false, false).unwrap().len(), 5);

        // Delete first 3
        let deleted = store.delete_batch(&ids[..3]).unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(store.list("proj1", None, false, false).unwrap().len(), 2);

        // Empty batch is a no-op
        assert_eq!(store.delete_batch(&[]).unwrap(), 0);
    }

    #[test]
    fn db_size_bytes_returns_nonzero() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let size = store.db_size_bytes();
        assert!(size.is_some());
        assert!(size.unwrap() > 0);
    }

    #[test]
    fn recall_hyphenated_term() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let id = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Replaced the legacy session-based authentication with OAuth2 tokens"
                        .into(),
                    reasoning: "Security audit required modern auth".into(),
                    tags: vec!["auth".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        assert!(id > 0);

        // Query with hyphenated term — should match both "session" and "based"
        let result = store
            .recall("proj1", &["session-based".to_string()], 10, 4096)
            .unwrap();
        assert_eq!(result.memories.len(), 1);
        assert_eq!(result.memories[0].id, id);

        // Query with just one segment should also match
        let result2 = store
            .recall("proj1", &["session".to_string()], 10, 4096)
            .unwrap();
        assert_eq!(result2.memories.len(), 1);
    }

    #[test]
    fn count_matches_list_for_small_datasets() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        for i in 0..5 {
            store
                .insert(
                    "proj1",
                    &ExtractedMemory {
                        kind: MemoryKind::Summary,
                        content: format!("Summary of run {i} with unique details"),
                        reasoning: "test".into(),
                        tags: vec![],
                    },
                    &format!("run{i}"),
                    "Claude",
                    &cfg,
                )
                .unwrap();
        }
        let listed = store.list("proj1", None, false, false).unwrap();
        let counted = store.count("proj1", None, false, false).unwrap();
        assert_eq!(listed.len() as u64, counted);
    }

    // -----------------------------------------------------------------------
    // Archival tests
    // -----------------------------------------------------------------------

    /// Helper: directly insert a memory with custom timestamps for archival testing.
    fn insert_raw_permanent(
        store: &MemoryStore,
        kind: &str,
        content: &str,
        created_at: &str,
        updated_at: &str,
        last_recalled_at: Option<&str>,
    ) -> i64 {
        let conn = store.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO memories (project_id, kind, content, reasoning, tags, \
             created_at, updated_at, last_recalled_at) \
             VALUES ('proj1', ?1, ?2, '', '', ?3, ?4, ?5)",
            rusqlite::params![kind, content, created_at, updated_at, last_recalled_at],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn archive_stale_permanent_archives_old_never_recalled() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let old = "2024-01-01T00:00:00+00:00";
        insert_raw_permanent(
            &store,
            "principle",
            "Old principle that was never recalled and should be archived",
            old,
            old,
            None,
        );
        let archived = store.archive_stale_permanent(365).unwrap();
        assert_eq!(archived, 1);
        // Should not appear in active list
        assert!(store.list("proj1", None, false, false).unwrap().is_empty());
        // Should appear in archived list
        assert_eq!(store.list("proj1", None, false, true).unwrap().len(), 1);
    }

    #[test]
    fn archive_stale_permanent_skips_recently_reinforced() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let old = "2024-01-01T00:00:00+00:00";
        let recent = chrono::Utc::now().to_rfc3339();
        // Created long ago but updated_at is recent (reinforced)
        insert_raw_permanent(
            &store,
            "principle",
            "Reinforced principle that should not be archived",
            old,
            &recent,
            None,
        );
        let archived = store.archive_stale_permanent(365).unwrap();
        assert_eq!(archived, 0);
        assert_eq!(store.list("proj1", None, false, false).unwrap().len(), 1);
    }

    #[test]
    fn archive_stale_permanent_disabled_when_zero() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let old = "2024-01-01T00:00:00+00:00";
        insert_raw_permanent(
            &store,
            "principle",
            "Old principle should remain because disabled",
            old,
            old,
            None,
        );
        let archived = store.archive_stale_permanent(0).unwrap();
        assert_eq!(archived, 0);
        assert_eq!(store.list("proj1", None, false, false).unwrap().len(), 1);
    }

    #[test]
    fn recall_excludes_archived() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let old = "2024-01-01T00:00:00+00:00";
        insert_raw_permanent(
            &store,
            "principle",
            "Connection pooling best practice for databases",
            old,
            old,
            None,
        );
        // Archive it
        store.archive_stale_permanent(365).unwrap();
        // Recall should not find it
        let recalled = store
            .recall("proj1", &["pooling".into(), "database".into()], 10, 16384)
            .unwrap();
        assert!(recalled.memories.is_empty());
    }

    #[test]
    fn list_archived_vs_active() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let old = "2024-01-01T00:00:00+00:00";
        // Insert one active and one that will be archived
        insert_raw_permanent(
            &store,
            "decision",
            "Active decision that should remain visible",
            old,
            &chrono::Utc::now().to_rfc3339(),
            None,
        );
        insert_raw_permanent(
            &store,
            "principle",
            "Stale principle should be archived after staleness check",
            old,
            old,
            None,
        );
        store.archive_stale_permanent(365).unwrap();

        let active = store.list("proj1", None, false, false).unwrap();
        assert_eq!(active.len(), 1);
        assert!(!active[0].archived);

        let archived = store.list("proj1", None, false, true).unwrap();
        assert_eq!(archived.len(), 1);
        assert!(archived[0].archived);
    }

    #[test]
    fn unarchive_restores_memory() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let old = "2024-01-01T00:00:00+00:00";

        // Test both branches: one with last_recalled_at NULL, one with non-NULL.
        let id_null = insert_raw_permanent(
            &store,
            "principle",
            "Archived principle with null recall",
            old,
            old,
            None,
        );
        let id_recalled = insert_raw_permanent(
            &store,
            "decision",
            "Archived decision with old recall timestamp",
            old,
            old,
            Some(old),
        );
        store.archive_stale_permanent(365).unwrap();
        assert!(store.list("proj1", None, false, false).unwrap().is_empty());

        // Unarchive both
        store.unarchive(id_null).unwrap();
        store.unarchive(id_recalled).unwrap();
        let active = store.list("proj1", None, false, false).unwrap();
        assert_eq!(active.len(), 2);
        assert!(active.iter().all(|m| !m.archived));

        // Re-run archival — both should stay active (durability check).
        let re_archived = store.archive_stale_permanent(365).unwrap();
        assert_eq!(
            re_archived, 0,
            "Unarchived memories must survive re-archival"
        );
        let still_active = store.list("proj1", None, false, false).unwrap();
        assert_eq!(still_active.len(), 2);
    }

    #[test]
    fn reinforce_archived_principle_unarchives() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let old = "2024-01-01T00:00:00+00:00";
        // Use non-NULL last_recalled_at to exercise the harder archival branch.
        insert_raw_permanent(
            &store,
            "principle",
            "Always validate user input at system boundaries",
            old,
            old,
            Some(old),
        );
        store.archive_stale_permanent(365).unwrap();
        assert!(store.list("proj1", None, false, false).unwrap().is_empty());

        // Re-extract a similar principle — should reinforce and auto-unarchive
        let mem = ExtractedMemory {
            kind: MemoryKind::Principle,
            content: "Always validate user input at system boundaries".into(),
            reasoning: "Security".into(),
            tags: vec![],
        };
        store.insert("proj1", &mem, "run2", "Claude", &cfg).unwrap();

        let active = store.list("proj1", None, false, false).unwrap();
        assert_eq!(active.len(), 1);
        assert!(!active[0].archived);
        assert!(active[0].evidence_count > 1);

        // Durability: re-run archival — reinforced principle must stay active.
        let re_archived = store.archive_stale_permanent(365).unwrap();
        assert_eq!(
            re_archived, 0,
            "Reinforced principle must survive re-archival"
        );
    }

    #[test]
    fn decision_dedup_unarchives() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let old = "2024-01-01T00:00:00+00:00";
        insert_raw_permanent(
            &store,
            "decision",
            "Use connection pooling with max twenty connections for PostgreSQL",
            old,
            old,
            None,
        );
        store.archive_stale_permanent(365).unwrap();
        assert!(store.list("proj1", None, false, false).unwrap().is_empty());

        // Re-extract same decision — should dedup and unarchive
        let mem = ExtractedMemory {
            kind: MemoryKind::Decision,
            content: "Use connection pooling with max twenty connections for PostgreSQL".into(),
            reasoning: "Performance".into(),
            tags: vec![],
        };
        let result = store.insert("proj1", &mem, "run2", "Claude", &cfg).unwrap();
        assert_eq!(result, -1); // dedup skip

        let active = store.list("proj1", None, false, false).unwrap();
        assert_eq!(active.len(), 1);
        assert!(!active[0].archived);
    }

    #[test]
    fn schema_migration_v5_to_v6() {
        // Create a real v5 database without the `archived` column,
        // then reopen via MemoryStore and verify the migration adds it.
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE memories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    content TEXT NOT NULL,
                    reasoning TEXT NOT NULL DEFAULT '',
                    source_run TEXT NOT NULL DEFAULT '',
                    source_agent TEXT NOT NULL DEFAULT '',
                    evidence_count INTEGER NOT NULL DEFAULT 1,
                    tags TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    updated_at TEXT NOT NULL,
                    recall_count INTEGER NOT NULL DEFAULT 0,
                    last_recalled_at TEXT
                );
                CREATE VIRTUAL TABLE memories_fts USING fts5(
                    content, reasoning, tags,
                    content=memories, content_rowid=id,
                    tokenize='porter unicode61'
                );
                CREATE TRIGGER memories_ai AFTER INSERT ON memories
                WHEN new.kind IN ('decision','principle')
                BEGIN
                    INSERT INTO memories_fts(rowid, content, reasoning, tags)
                    VALUES (new.id, new.content, new.reasoning, new.tags);
                END;
                CREATE TRIGGER memories_au AFTER UPDATE ON memories
                WHEN new.kind IN ('decision','principle')
                BEGIN
                    INSERT INTO memories_fts(memories_fts, rowid, content, reasoning, tags)
                    VALUES ('delete', old.id, old.content, old.reasoning, old.tags);
                    INSERT INTO memories_fts(rowid, content, reasoning, tags)
                    VALUES (new.id, new.content, new.reasoning, new.tags);
                END;
                CREATE TRIGGER memories_ad AFTER DELETE ON memories BEGIN
                    INSERT INTO memories_fts(memories_fts, rowid, content, reasoning, tags)
                    VALUES ('delete', old.id, old.content, old.reasoning, old.tags);
                END;
                CREATE INDEX idx_memories_project ON memories(project_id);
                CREATE INDEX idx_memories_project_kind ON memories(project_id, kind);
                CREATE INDEX idx_memories_project_recall ON memories(project_id, recall_count DESC, last_recalled_at DESC);
                CREATE INDEX idx_memories_expires ON memories(expires_at) WHERE expires_at IS NOT NULL;
                PRAGMA user_version = 5;",
            )
            .unwrap();
            // Insert a permanent memory so we can verify it survives and gets archived column.
            conn.execute(
                "INSERT INTO memories (project_id, kind, content, reasoning, created_at, updated_at)
                 VALUES ('p', 'decision', 'Use connection pooling', 'performance', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')",
                [],
            )
            .unwrap();
        }

        // Reopen through MemoryStore — should auto-migrate 5→6.
        let store = MemoryStore::open(&db_path).unwrap();
        let conn = store.conn.lock().unwrap();

        // Version should be 6.
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // archived column should exist with default 0.
        let archived: bool = conn
            .query_row("SELECT archived FROM memories WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(!archived, "Pre-existing row should default to not archived");

        // Data should survive.
        drop(conn);
        let list = store.list("p", None, false, false).unwrap();
        assert_eq!(list.len(), 1);
        assert!(list[0].content.contains("pooling"));

        // archive_stale_permanent should work (the row is >365 days old).
        let archived_count = store.archive_stale_permanent(365).unwrap();
        assert_eq!(archived_count, 1);
        let active = store.list("p", None, false, false).unwrap();
        assert!(active.is_empty());
        let archived_list = store.list("p", None, false, true).unwrap();
        assert_eq!(archived_list.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Unit tests for FTS5 escape functions
    // -----------------------------------------------------------------------

    #[test]
    fn fts5_segment_empty() {
        assert_eq!(escape_fts5_segment(""), None);
    }

    #[test]
    fn fts5_segment_all_punctuation() {
        assert_eq!(escape_fts5_segment("!@#$%^&*()"), None);
    }

    #[test]
    fn fts5_segment_short_exact_only() {
        // < 5 chars: exact quoted match only, no prefix wildcard
        assert_eq!(escape_fts5_segment("rust"), Some("\"rust\"".to_string()));
    }

    #[test]
    fn fts5_segment_long_includes_prefix() {
        // >= 5 chars: both exact and prefix wildcard
        assert_eq!(
            escape_fts5_segment("session"),
            Some("(\"session\" OR session*)".to_string())
        );
    }

    #[test]
    fn fts5_segment_strips_special_chars() {
        assert_eq!(
            escape_fts5_segment("hello!world"),
            Some("(\"helloworld\" OR helloworld*)".to_string())
        );
    }

    #[test]
    fn fts5_segment_single_char() {
        assert_eq!(escape_fts5_segment("x"), Some("\"x\"".to_string()));
    }

    #[test]
    fn fts5_segment_underscores_preserved() {
        assert_eq!(
            escape_fts5_segment("my_var"),
            Some("(\"my_var\" OR my_var*)".to_string())
        );
    }

    #[test]
    fn fts5_term_empty() {
        assert_eq!(escape_fts5_term(""), None);
    }

    #[test]
    fn fts5_term_single_hyphen() {
        assert_eq!(escape_fts5_term("-"), None);
    }

    #[test]
    fn fts5_term_simple_word() {
        assert_eq!(
            escape_fts5_term("config"),
            Some("(\"config\" OR config*)".to_string())
        );
    }

    #[test]
    fn fts5_term_hyphenated_pair() {
        let result = escape_fts5_term("session-based").unwrap();
        assert!(result.contains("AND"));
        assert!(result.contains("session"));
        assert!(result.contains("based"));
    }

    #[test]
    fn fts5_term_deeply_nested_hyphens() {
        let result = escape_fts5_term("a-b-c-d-e").unwrap();
        // 5 segments, all ANDed together
        assert_eq!(result.matches("AND").count(), 4);
    }

    #[test]
    fn fts5_term_leading_trailing_hyphens() {
        // "-hello-" splits into ["", "hello", ""]
        // empty segments produce None from escape_fts5_segment
        let result = escape_fts5_term("-hello-").unwrap();
        assert!(result.contains("hello"));
        assert!(!result.contains("AND"));
    }

    #[test]
    fn fts5_term_all_empty_segments() {
        // "---" splits into all empty strings
        assert_eq!(escape_fts5_term("---"), None);
    }
}

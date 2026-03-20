use super::*;
use std::collections::HashSet;

/// Internal memory files that should be hidden from the results tree.
/// Uses an explicit list (not blanket `_`-prefix) to avoid hiding
/// `_errors.log` and agent outputs from sanitized names like `_Claude`.
const MEMORY_INTERNAL_FILES: &[&str] = &["_recalled_memories.md", "_memories.json"];

fn is_memory_internal_file(path: &std::path::Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| MEMORY_INTERNAL_FILES.contains(&n))
}

pub(super) fn load_results(app: &mut App) {
    let Some(run_dir) = app.running.run_dir.clone() else {
        app.results.reset_to_home();
        return;
    };

    app.results.results_loading = true;
    app.results.results_load_rx = None;
    app.results.result_cursor = 0;
    app.results.result_preview = "Loading results...".into();
    let (tx, rx) = mpsc::unbounded_channel();
    app.results.results_load_rx = Some(rx);

    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || load_results_sync(run_dir)).await;
        let final_result = match result {
            Ok(inner) => inner,
            Err(e) => Err(format!("Results load task failed: {e}")),
        };
        let _ = tx.send(final_result);
    });
}

pub(super) fn handle_results_load_result(
    app: &mut App,
    result: Result<crate::app::ResultsLoadPayload, String>,
) {
    app.results.results_loading = false;
    app.results.results_load_rx = None;

    match result {
        Ok(payload) => {
            app.results.batch_result_finalization_expanded =
                !payload.batch_result_finalization_files.is_empty();
            app.results.batch_result_finalization_files = payload.batch_result_finalization_files;
            app.results.result_files = payload.result_files;
            app.results.batch_result_runs = payload.batch_result_runs;
            app.results.batch_result_root_files = payload.batch_result_root_files;
            app.results.batch_result_expanded = payload.batch_result_expanded;
            app.results.result_cursor = 0;
            update_preview(app);
        }
        Err(error) => {
            app.results.reset_to_home();
            app.error_modal = Some(error);
        }
    }
}

pub(super) fn update_preview(app: &mut App) {
    let selected = selected_preview_path(app).cloned();
    app.results.preview_request_id = app.results.preview_request_id.wrapping_add(1);
    let request_id = app.results.preview_request_id;
    app.results.preview_loading = true;
    app.results.preview_rx = None;
    app.results.result_preview = if selected.is_some() {
        "Loading preview...".into()
    } else {
        String::new()
    };

    let (tx, rx) = mpsc::unbounded_channel();
    app.results.preview_rx = Some(rx);
    tokio::spawn(async move {
        let preview = match selected {
            Some(path) => tokio::fs::read_to_string(path)
                .await
                .unwrap_or_else(|e| format!("Error: {e}")),
            None => String::new(),
        };
        let _ = tx.send(crate::app::PreviewLoadResult {
            request_id,
            preview,
        });
    });
}

pub(super) fn handle_preview_result(app: &mut App, result: crate::app::PreviewLoadResult) {
    if result.request_id != app.results.preview_request_id {
        return;
    }

    app.results.preview_loading = false;
    app.results.preview_rx = None;
    app.results.result_preview = result.preview;
}

fn load_results_sync(
    run_dir: std::path::PathBuf,
) -> Result<crate::app::ResultsLoadPayload, String> {
    if OutputManager::is_batch_root(&run_dir) {
        let mut root_files = Vec::new();
        let mut run_groups = Vec::new();
        let entries = std::fs::read_dir(&run_dir)
            .map_err(|e| format!("Failed to read {}: {e}", run_dir.display()))?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if !is_memory_internal_file(&path) {
                    root_files.push(path);
                }
                continue;
            }

            if !path.is_dir() {
                continue;
            }

            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(run_id) = name
                .strip_prefix("run_")
                .and_then(|n| n.parse::<u32>().ok())
            else {
                continue;
            };

            let mut files = std::fs::read_dir(&path)
                .map_err(|e| format!("Failed to read {}: {e}", path.display()))?
                .flatten()
                .filter_map(|entry| {
                    let path = entry.path();
                    if !path.is_file() || is_memory_internal_file(&path) {
                        return None;
                    }
                    Some(path)
                })
                .collect::<Vec<_>>();

            // Collect files from sub_* (sub-pipeline) directories and their finalization/ subdirs
            for sub_dir in crate::post_run::sub_pipeline_directories(&path) {
                if let Ok(entries) = std::fs::read_dir(&sub_dir) {
                    for entry in entries.flatten() {
                        let p = entry.path();
                        if p.is_file() && !is_memory_internal_file(&p) {
                            files.push(p);
                        }
                    }
                }
                let sub_fin = sub_dir.join("finalization");
                if sub_fin.is_dir() {
                    if let Ok(entries) = std::fs::read_dir(&sub_fin) {
                        for entry in entries.flatten() {
                            let p = entry.path();
                            if p.is_file() && !is_memory_internal_file(&p) {
                                files.push(p);
                            }
                        }
                    }
                }
            }

            files.sort();
            run_groups.push(BatchRunGroup { run_id, files });
        }

        root_files.sort();
        run_groups.sort_by_key(|group| group.run_id);
        let batch_result_expanded = run_groups.iter().map(|group| group.run_id).collect();

        // Collect finalization files from batch root's finalization/ directory
        let mut finalization_files = Vec::new();
        let fin_dir = run_dir.join("finalization");
        if fin_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&fin_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() && !is_memory_internal_file(&path) {
                        finalization_files.push(path);
                    }
                }
            }
            finalization_files.sort();
        }

        return Ok(crate::app::ResultsLoadPayload {
            result_files: Vec::new(),
            batch_result_runs: run_groups,
            batch_result_root_files: root_files,
            batch_result_expanded,
            batch_result_finalization_files: finalization_files,
        });
    }

    let mut files = std::fs::read_dir(&run_dir)
        .map_err(|e| format!("Failed to read {}: {e}", run_dir.display()))?
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if !path.is_file() || is_memory_internal_file(&path) {
                return None;
            }
            Some(path)
        })
        .collect::<Vec<_>>();

    // Collect finalization files if the finalization/ subdirectory exists
    let fin_dir = run_dir.join("finalization");
    if fin_dir.is_dir() {
        if let Ok(entries) = std::fs::read_dir(&fin_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && !is_memory_internal_file(&path) {
                    files.push(path);
                }
            }
        }
    }

    // Collect files from sub_* (sub-pipeline) directories and their finalization/ subdirs
    for sub_dir in crate::post_run::sub_pipeline_directories(&run_dir) {
        if let Ok(entries) = std::fs::read_dir(&sub_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && !is_memory_internal_file(&path) {
                    files.push(path);
                }
            }
        }
        let sub_fin = sub_dir.join("finalization");
        if sub_fin.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&sub_fin) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() && !is_memory_internal_file(&path) {
                        files.push(path);
                    }
                }
            }
        }
    }

    files.sort();

    Ok(crate::app::ResultsLoadPayload {
        result_files: files,
        batch_result_runs: Vec::new(),
        batch_result_root_files: Vec::new(),
        batch_result_expanded: HashSet::new(),
        batch_result_finalization_files: Vec::new(),
    })
}

fn selected_preview_path(app: &App) -> Option<&std::path::PathBuf> {
    if has_batch_result_tree(app) {
        match batch_result_entry_at(app, app.results.result_cursor) {
            Some(BatchResultEntry::File(path)) => Some(path),
            _ => None,
        }
    } else {
        app.results.result_files.get(app.results.result_cursor)
    }
}

pub(super) fn has_batch_result_tree(app: &App) -> bool {
    !app.results.batch_result_runs.is_empty()
        || !app.results.batch_result_root_files.is_empty()
        || !app.results.batch_result_finalization_files.is_empty()
}

pub(super) enum BatchResultEntry<'a> {
    RunHeader(u32),
    FinalizationHeader,
    File(&'a std::path::PathBuf),
}

pub(super) fn batch_result_visible_len(app: &App) -> usize {
    let mut len = app.results.batch_result_root_files.len();
    for group in &app.results.batch_result_runs {
        len += 1;
        if app.results.batch_result_expanded.contains(&group.run_id) {
            len += group.files.len();
        }
    }
    // Finalization group (header + files) when finalization files exist
    if !app.results.batch_result_finalization_files.is_empty() {
        len += 1; // FinalizationHeader
        if app.results.batch_result_finalization_expanded {
            len += app.results.batch_result_finalization_files.len();
        }
    }
    len
}

pub(super) fn batch_result_entry_at(app: &App, mut index: usize) -> Option<BatchResultEntry<'_>> {
    for group in &app.results.batch_result_runs {
        if index == 0 {
            return Some(BatchResultEntry::RunHeader(group.run_id));
        }
        index -= 1;

        if app.results.batch_result_expanded.contains(&group.run_id) {
            if index < group.files.len() {
                return Some(BatchResultEntry::File(&group.files[index]));
            }
            index = index.saturating_sub(group.files.len());
        }
    }

    // Finalization group (between run groups and root files)
    if !app.results.batch_result_finalization_files.is_empty() {
        if index == 0 {
            return Some(BatchResultEntry::FinalizationHeader);
        }
        index -= 1;

        if app.results.batch_result_finalization_expanded {
            if index < app.results.batch_result_finalization_files.len() {
                return Some(BatchResultEntry::File(
                    &app.results.batch_result_finalization_files[index],
                ));
            }
            index = index.saturating_sub(app.results.batch_result_finalization_files.len());
        }
    }

    if index < app.results.batch_result_root_files.len() {
        Some(BatchResultEntry::File(
            &app.results.batch_result_root_files[index],
        ))
    } else {
        None
    }
}

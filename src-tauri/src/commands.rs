use crate::indexer::{IndexerError, SharedLogFile};
use crate::query_engine::{FileFormat, QueryEngine, QueryResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tauri::{AppHandle, Emitter, State};

/// Application state shared across commands
pub struct AppState {
    pub log_file: SharedLogFile,
    pub query_engine: QueryEngine,
}

impl AppState {
    pub async fn new() -> Self {
        let query_engine = QueryEngine::new();
        query_engine.register_udfs().await.ok();

        AppState {
            log_file: SharedLogFile::new(),
            query_engine,
        }
    }
}

/// File information returned when opening a file
#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    pub line_count: u64,
    pub format: String,
}

/// Progress event for indexing
#[derive(Clone, Serialize)]
pub struct IndexProgress {
    pub phase: String,
    pub progress: f32,
    pub message: String,
}

/// Error type for Tauri commands
#[derive(Debug, Serialize)]
pub struct CommandError {
    pub message: String,
}

impl From<IndexerError> for CommandError {
    fn from(err: IndexerError) -> Self {
        CommandError {
            message: err.to_string(),
        }
    }
}

impl From<crate::query_engine::QueryError> for CommandError {
    fn from(err: crate::query_engine::QueryError) -> Self {
        CommandError {
            message: err.to_string(),
        }
    }
}

impl From<std::io::Error> for CommandError {
    fn from(err: std::io::Error) -> Self {
        CommandError {
            message: err.to_string(),
        }
    }
}

/// Open a log file and build the index
#[tauri::command]
pub async fn open_file(
    path: String,
    state: State<'_, Arc<AppState>>,
    app: AppHandle,
) -> Result<FileInfo, CommandError> {
    // Emit progress event for indexing start
    app.emit(
        "index-progress",
        IndexProgress {
            phase: "opening".to_string(),
            progress: 0.0,
            message: "Opening file...".to_string(),
        },
    )
    .ok();

    // Open and index the file
    state.log_file.open(&path)?;

    // Get file info
    let (file_size, line_count) = state
        .log_file
        .with_file(|f| (f.file_size(), f.line_count()))
        .unwrap_or((0, 0));

    app.emit(
        "index-progress",
        IndexProgress {
            phase: "indexing".to_string(),
            progress: 0.5,
            message: format!("Indexing {} lines...", line_count),
        },
    )
    .ok();

    // Detect file format
    let format = QueryEngine::detect_format(&path).unwrap_or(FileFormat::PlainText);

    // Register with query engine
    state
        .query_engine
        .register_table(&path, "logs")
        .await
        .ok();

    app.emit(
        "index-progress",
        IndexProgress {
            phase: "complete".to_string(),
            progress: 1.0,
            message: "File ready".to_string(),
        },
    )
    .ok();

    Ok(FileInfo {
        path,
        size: file_size,
        line_count,
        format: format!("{:?}", format),
    })
}

/// Close the current file
#[tauri::command]
pub async fn close_file(state: State<'_, Arc<AppState>>) -> Result<(), CommandError> {
    state.log_file.close();
    state.query_engine.clear().await;
    Ok(())
}

/// Get a range of lines from the file
#[tauri::command]
pub fn get_lines(
    start: u64,
    count: u64,
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<String>, CommandError> {
    state
        .log_file
        .with_file(|f| f.get_lines(start, count))
        .ok_or_else(|| CommandError {
            message: "No file open".to_string(),
        })?
        .map_err(CommandError::from)
}

/// Get lines in binary format for efficient transfer
#[tauri::command]
pub fn get_lines_binary(
    start: u64,
    count: u64,
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<u8>, CommandError> {
    state
        .log_file
        .with_file(|f| f.get_lines_binary(start, count))
        .ok_or_else(|| CommandError {
            message: "No file open".to_string(),
        })?
        .map_err(CommandError::from)
}

/// Get file information
#[tauri::command]
pub fn get_file_info(state: State<'_, Arc<AppState>>) -> Result<Option<FileInfo>, CommandError> {
    Ok(state.log_file.with_file(|f| FileInfo {
        path: f.path().to_string(),
        size: f.file_size(),
        line_count: f.line_count(),
        format: "Unknown".to_string(),
    }))
}

/// Search for a pattern in the file
#[tauri::command]
pub fn search(
    pattern: String,
    max_results: Option<usize>,
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<u64>, CommandError> {
    let max = max_results.unwrap_or(1000);
    state
        .log_file
        .with_file(|f| f.search(&pattern, max))
        .ok_or_else(|| CommandError {
            message: "No file open".to_string(),
        })?
        .map_err(CommandError::from)
}

/// Execute a SQL query
#[tauri::command]
pub async fn execute_sql(
    query: String,
    state: State<'_, Arc<AppState>>,
) -> Result<QueryResult, CommandError> {
    state
        .query_engine
        .execute_sql(&query)
        .await
        .map_err(CommandError::from)
}

/// Get the total line count
#[tauri::command]
pub fn get_line_count(state: State<'_, Arc<AppState>>) -> Result<u64, CommandError> {
    state
        .log_file
        .with_file(|f| f.line_count())
        .ok_or_else(|| CommandError {
            message: "No file open".to_string(),
        })
}

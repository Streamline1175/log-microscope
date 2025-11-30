pub mod commands;
pub mod indexer;
pub mod query_engine;

use commands::AppState;
use std::sync::Arc;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // Create a runtime for async initialization
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
    let app_state = rt.block_on(async { Arc::new(AppState::new().await) });

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .manage(app_state)
        .invoke_handler(tauri::generate_handler![
            commands::open_file,
            commands::close_file,
            commands::get_lines,
            commands::get_lines_binary,
            commands::get_file_info,
            commands::search,
            commands::execute_sql,
            commands::get_line_count,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

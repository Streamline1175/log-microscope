use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
use datafusion::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use datafusion::arrow::error::ArrowError;
use thiserror::Error;
use tokio::sync::Mutex;

/// Errors that can occur during query operations
#[derive(Error, Debug)]
pub enum QueryError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Failed to open file: {0}")]
    FileOpen(#[from] std::io::Error),
    #[error("No file registered")]
    NoFile,
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// File format detected for a log file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileFormat {
    PlainText,
    Ndjson,
    Csv,
}

/// Result of a SQL query execution
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
}

/// SQL query engine powered by Apache DataFusion
pub struct QueryEngine {
    ctx: Mutex<SessionContext>,
    registered_table: Mutex<Option<String>>,
}

impl QueryEngine {
    /// Create a new query engine with optimized configuration
    pub fn new() -> Self {
        let config = SessionConfig::new()
            .with_batch_size(8192)
            .with_target_partitions(num_cpus::get())
            .with_information_schema(true);

        let ctx = SessionContext::new_with_config(config);

        QueryEngine {
            ctx: Mutex::new(ctx),
            registered_table: Mutex::new(None),
        }
    }

    /// Detect the format of a file by examining its content
    pub fn detect_format<P: AsRef<Path>>(path: P) -> Result<FileFormat, QueryError> {
        let content = std::fs::read_to_string(&path)?;
        let first_lines: Vec<&str> = content.lines().take(10).collect();

        if first_lines.is_empty() {
            return Ok(FileFormat::PlainText);
        }

        // Check for NDJSON (lines starting with { and ending with })
        let json_lines = first_lines
            .iter()
            .filter(|line| {
                let trimmed = line.trim();
                trimmed.starts_with('{') && trimmed.ends_with('}')
            })
            .count();

        if json_lines > first_lines.len() / 2 {
            return Ok(FileFormat::Ndjson);
        }

        // Check for CSV (consistent comma count across lines)
        let comma_counts: Vec<usize> = first_lines
            .iter()
            .map(|line| line.matches(',').count())
            .collect();

        if comma_counts.len() > 1 {
            let first_count = comma_counts[0];
            if first_count > 0 && comma_counts.iter().all(|&c| c == first_count) {
                return Ok(FileFormat::Csv);
            }
        }

        Ok(FileFormat::PlainText)
    }

    /// Register a table from a file path
    pub async fn register_table<P: AsRef<Path> + Send>(
        &self,
        path: P,
        table_name: &str,
    ) -> Result<FileFormat, QueryError> {
        let path = path.as_ref();
        let format = Self::detect_format(path)?;
        let path_str = path.to_string_lossy().to_string();
        let table_name = table_name.to_string();

        let ctx = self.ctx.lock().await;

        // For all formats, we create an in-memory table with line_number and line columns
        // This gives us consistent querying regardless of format
        let file = File::open(&path_str)?;
        let reader = BufReader::new(file);
        
        // Read lines in batches to create Arrow arrays
        const BATCH_SIZE: usize = 100_000;
        let mut all_batches = Vec::new();
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("line_number", DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        
        let mut line_numbers: Vec<i64> = Vec::with_capacity(BATCH_SIZE);
        let mut lines: Vec<String> = Vec::with_capacity(BATCH_SIZE);
        let mut current_line: i64 = 1;
        
        for line_result in reader.lines() {
            let line = line_result.unwrap_or_default();
            line_numbers.push(current_line);
            lines.push(line);
            current_line += 1;
            
            if line_numbers.len() >= BATCH_SIZE {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(std::mem::take(&mut line_numbers))) as ArrayRef,
                        Arc::new(StringArray::from(std::mem::take(&mut lines))) as ArrayRef,
                    ],
                )?;
                all_batches.push(batch);
                line_numbers = Vec::with_capacity(BATCH_SIZE);
                lines = Vec::with_capacity(BATCH_SIZE);
            }
        }
        
        // Don't forget the last batch
        if !line_numbers.is_empty() {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(line_numbers)) as ArrayRef,
                    Arc::new(StringArray::from(lines)) as ArrayRef,
                ],
            )?;
            all_batches.push(batch);
        }
        
        // Create a MemTable from the batches
        let mem_table = MemTable::try_new(schema, vec![all_batches])?;
        ctx.register_table(&table_name, Arc::new(mem_table))?;

        drop(ctx);
        *self.registered_table.lock().await = Some(table_name);

        Ok(format)
    }

    /// Register custom UDFs for log analysis
    pub async fn register_udfs(&self) -> Result<(), QueryError> {
        let ctx = self.ctx.lock().await;

        // regex_match UDF
        let regex_match = create_udf(
            "regex_match",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| {
                let text_array = match &args[0] {
                    ColumnarValue::Array(arr) => arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal("Expected string array".into()))?
                        .clone(),
                    ColumnarValue::Scalar(scalar) => {
                        let s = scalar.to_string();
                        StringArray::from(vec![s.as_str()])
                    }
                };

                let pattern = match &args[1] {
                    ColumnarValue::Scalar(scalar) => scalar.to_string(),
                    _ => return Err(DataFusionError::Internal("Pattern must be scalar".into())),
                };

                let regex = Regex::new(&pattern)
                    .map_err(|e| DataFusionError::Internal(format!("Invalid regex: {}", e)))?;

                let result: datafusion::arrow::array::BooleanArray = text_array
                    .iter()
                    .map(|opt| opt.map(|s| regex.is_match(s)))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }),
        );

        ctx.register_udf(regex_match);

        // json_extract UDF for extracting values from JSON strings
        let json_extract = create_udf(
            "json_extract",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| {
                let json_array = match &args[0] {
                    ColumnarValue::Array(arr) => arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal("Expected string array".into()))?
                        .clone(),
                    ColumnarValue::Scalar(scalar) => {
                        let s = scalar.to_string();
                        StringArray::from(vec![s.as_str()])
                    }
                };

                let key = match &args[1] {
                    ColumnarValue::Scalar(scalar) => scalar.to_string().trim_matches('"').to_string(),
                    _ => return Err(DataFusionError::Internal("Key must be scalar".into())),
                };

                let result: StringArray = json_array
                    .iter()
                    .map(|opt| {
                        opt.and_then(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .ok()
                                .and_then(|v| v.get(&key).map(|v| v.to_string()))
                        })
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }),
        );

        ctx.register_udf(json_extract);

        Ok(())
    }

    /// Execute a SQL query and return the results
    pub async fn execute_sql(&self, query: &str) -> Result<QueryResult, QueryError> {
        let ctx = self.ctx.lock().await;
        let df = ctx.sql(query).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
            });
        }

        // Get column names from schema
        let schema = batches[0].schema();
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        // Convert record batches to rows
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();

        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut row: Vec<serde_json::Value> = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let value = Self::extract_value(batch.column(col_idx), row_idx);
                    row.push(value);
                }
                rows.push(row);
            }
        }

        let row_count = rows.len();

        Ok(QueryResult {
            columns,
            rows,
            row_count,
        })
    }

    /// Extract a value from an Arrow array at a specific index
    fn extract_value(array: &ArrayRef, index: usize) -> serde_json::Value {
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::DataType;

        if array.is_null(index) {
            return serde_json::Value::Null;
        }

        match array.data_type() {
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                serde_json::Value::String(arr.value(index).to_string())
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                serde_json::Value::String(arr.value(index).to_string())
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                serde_json::json!(arr.value(index))
            }
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                serde_json::json!(arr.value(index))
            }
            _ => serde_json::Value::String(format!("{:?}", array.data_type())),
        }
    }

    /// Clear all registered tables
    pub async fn clear(&self) {
        *self.registered_table.lock().await = None;
        *self.ctx.lock().await = SessionContext::new();
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_json_file() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{"level":"info","message":"test1"}}"#).unwrap();
        writeln!(file, r#"{{"level":"error","message":"test2"}}"#).unwrap();
        writeln!(file, r#"{{"level":"info","message":"test3"}}"#).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_detect_format_ndjson() {
        let file = create_test_json_file();
        let format = QueryEngine::detect_format(file.path()).unwrap();
        assert_eq!(format, FileFormat::Ndjson);
    }

    #[test]
    fn test_detect_format_plain_text() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "2024-01-01 INFO Starting application").unwrap();
        writeln!(file, "2024-01-01 ERROR Something went wrong").unwrap();
        file.flush().unwrap();

        let format = QueryEngine::detect_format(file.path()).unwrap();
        assert_eq!(format, FileFormat::PlainText);
    }

    #[test]
    fn test_detect_format_csv() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "timestamp,level,message").unwrap();
        writeln!(file, "2024-01-01,INFO,test1").unwrap();
        writeln!(file, "2024-01-01,ERROR,test2").unwrap();
        file.flush().unwrap();

        let format = QueryEngine::detect_format(file.path()).unwrap();
        assert_eq!(format, FileFormat::Csv);
    }
}

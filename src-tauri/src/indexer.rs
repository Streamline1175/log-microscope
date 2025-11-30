use memchr::memchr_iter;
use memmap2::Mmap;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

/// Errors that can occur during log file operations
#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("Failed to open file: {0}")]
    FileOpen(#[from] std::io::Error),
    #[error("File is empty")]
    EmptyFile,
    #[error("Invalid line range: start={0}, count={1}, total_lines={2}")]
    InvalidRange(u64, u64, u64),
}

/// Result of chunk processing during parallel indexing
struct ChunkResult {
    offsets: Vec<u64>,
}

/// A memory-mapped log file with pre-built line index for O(1) access
pub struct LogFile {
    mmap: Mmap,
    /// Line offsets - each entry is the byte offset where a line starts
    line_offsets: Vec<u64>,
    /// File size in bytes
    file_size: u64,
    /// File path
    path: String,
}

impl LogFile {
    /// Open a log file and build the line index
    /// Uses memory mapping for zero-copy access and parallel indexing for speed
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IndexerError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let file = File::open(&path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();

        if file_size == 0 {
            return Err(IndexerError::EmptyFile);
        }

        // Safety: We're opening in read-only mode and the file exists
        let mmap = unsafe { Mmap::map(&file)? };

        // Build the line index using parallel processing
        let line_offsets = Self::build_index(&mmap);

        Ok(LogFile {
            mmap,
            line_offsets,
            file_size,
            path: path_str,
        })
    }

    /// Build line index using parallel SIMD-accelerated scanning
    /// Divides the file into chunks and processes them in parallel using rayon
    fn build_index(data: &[u8]) -> Vec<u64> {
        let data_len = data.len();
        if data_len == 0 {
            return vec![0];
        }

        // Determine optimal chunk size based on CPU cores
        // Target ~64MB chunks for good parallelism without excessive overhead
        let num_cores = rayon::current_num_threads();
        let chunk_size = std::cmp::max(64 * 1024 * 1024, data_len / num_cores);

        // Calculate chunk boundaries
        let chunks: Vec<(usize, usize)> = (0..data_len)
            .step_by(chunk_size)
            .map(|start| {
                let end = std::cmp::min(start + chunk_size, data_len);
                (start, end)
            })
            .collect();

        // Process chunks in parallel using SIMD-accelerated memchr
        let chunk_results: Vec<ChunkResult> = chunks
            .par_iter()
            .map(|&(start, end)| {
                let chunk = &data[start..end];
                let mut offsets = Vec::new();

                // Use SIMD-accelerated newline search (memchr processes 32 bytes at a time)
                for pos in memchr_iter(b'\n', chunk) {
                    // Store the position after the newline (start of next line)
                    let absolute_pos = start as u64 + pos as u64 + 1;
                    if absolute_pos < data_len as u64 {
                        offsets.push(absolute_pos);
                    }
                }

                ChunkResult {
                    offsets,
                }
            })
            .collect();

        // Reconcile chunk results into global index
        let mut global_index = Vec::with_capacity(data_len / 100); // Estimate ~100 bytes per line
        global_index.push(0); // First line always starts at offset 0

        for result in chunk_results {
            global_index.extend(result.offsets);
        }

        // Sort to ensure correct order (chunks may complete out of order)
        global_index.sort_unstable();
        global_index.dedup();

        global_index
    }

    /// Get the total number of lines in the file
    pub fn line_count(&self) -> u64 {
        self.line_offsets.len() as u64
    }

    /// Get the file size in bytes
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get the file path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get a range of lines from the file
    /// Returns a vector of strings for each line
    pub fn get_lines(&self, start: u64, count: u64) -> Result<Vec<String>, IndexerError> {
        let total_lines = self.line_count();
        
        if start >= total_lines {
            return Ok(vec![]);
        }

        let actual_count = std::cmp::min(count, total_lines - start);
        let mut lines = Vec::with_capacity(actual_count as usize);

        for i in 0..actual_count {
            let line_idx = (start + i) as usize;
            let line_start = self.line_offsets[line_idx] as usize;
            
            // Determine line end (next line start - 1, or end of file)
            let line_end = if line_idx + 1 < self.line_offsets.len() {
                self.line_offsets[line_idx + 1] as usize - 1 // Exclude newline
            } else {
                self.mmap.len() // Last line goes to end of file
            };

            // Handle potential \r\n line endings
            let actual_end = if line_end > line_start && line_end <= self.mmap.len() {
                let end = std::cmp::min(line_end, self.mmap.len());
                if end > 0 && self.mmap[end - 1] == b'\r' {
                    end - 1
                } else {
                    end
                }
            } else {
                line_start
            };

            // Extract the line bytes and convert to string
            if line_start <= actual_end && actual_end <= self.mmap.len() {
                let line_bytes = &self.mmap[line_start..actual_end];
                // Use lossy conversion to handle potential invalid UTF-8
                lines.push(String::from_utf8_lossy(line_bytes).to_string());
            } else {
                lines.push(String::new());
            }
        }

        Ok(lines)
    }

    /// Get lines as binary data with a header containing line lengths
    /// Format: [num_lines: u32][len1: u32][len2: u32]...[data]
    /// This is more efficient than JSON for large data transfers
    pub fn get_lines_binary(&self, start: u64, count: u64) -> Result<Vec<u8>, IndexerError> {
        let lines = self.get_lines(start, count)?;
        
        // Calculate total size needed
        let header_size = 4 + (lines.len() * 4); // num_lines + lengths
        let data_size: usize = lines.iter().map(|l| l.len()).sum();
        let total_size = header_size + data_size;

        let mut buffer = Vec::with_capacity(total_size);

        // Write number of lines
        buffer.extend_from_slice(&(lines.len() as u32).to_le_bytes());

        // Write line lengths
        for line in &lines {
            buffer.extend_from_slice(&(line.len() as u32).to_le_bytes());
        }

        // Write line data
        for line in &lines {
            buffer.extend_from_slice(line.as_bytes());
        }

        Ok(buffer)
    }

    /// Search for a pattern in the file using parallel regex matching
    /// Returns line numbers that match the pattern
    pub fn search(&self, pattern: &str, max_results: usize) -> Result<Vec<u64>, IndexerError> {
        let regex = regex::Regex::new(pattern)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

        let total_lines = self.line_count();
        let results = Arc::new(RwLock::new(Vec::new()));

        // Process lines in parallel chunks
        let chunk_size = 10000;
        let chunks: Vec<u64> = (0..total_lines).step_by(chunk_size).collect();

        chunks.par_iter().for_each(|&chunk_start| {
            let chunk_end = std::cmp::min(chunk_start + chunk_size as u64, total_lines);
            let mut local_results = Vec::new();

            for line_num in chunk_start..chunk_end {
                // Early exit if we have enough results
                {
                    let r = results.read();
                    if r.len() >= max_results {
                        return;
                    }
                }

                let line_idx = line_num as usize;
                let line_start = self.line_offsets[line_idx] as usize;
                let line_end = if line_idx + 1 < self.line_offsets.len() {
                    self.line_offsets[line_idx + 1] as usize
                } else {
                    self.mmap.len()
                };

                if line_start < line_end && line_end <= self.mmap.len() {
                    let line_bytes = &self.mmap[line_start..line_end];
                    if let Ok(line_str) = std::str::from_utf8(line_bytes) {
                        if regex.is_match(line_str) {
                            local_results.push(line_num);
                        }
                    }
                }
            }

            // Merge local results into global results
            if !local_results.is_empty() {
                let mut r = results.write();
                r.extend(local_results);
            }
        });

        let mut final_results = Arc::try_unwrap(results)
            .map(|rw| rw.into_inner())
            .unwrap_or_else(|arc| arc.read().clone());
        
        final_results.sort_unstable();
        final_results.truncate(max_results);
        
        Ok(final_results)
    }

    /// Get raw access to the memory-mapped data (for DataFusion integration)
    pub fn data(&self) -> &[u8] {
        &self.mmap
    }
}

/// Thread-safe wrapper for LogFile that can be shared across threads
pub struct SharedLogFile {
    inner: RwLock<Option<LogFile>>,
}

impl SharedLogFile {
    pub fn new() -> Self {
        SharedLogFile {
            inner: RwLock::new(None),
        }
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<(), IndexerError> {
        let log_file = LogFile::open(path)?;
        *self.inner.write() = Some(log_file);
        Ok(())
    }

    pub fn close(&self) {
        *self.inner.write() = None;
    }

    pub fn is_open(&self) -> bool {
        self.inner.read().is_some()
    }

    pub fn with_file<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&LogFile) -> R,
    {
        self.inner.read().as_ref().map(f)
    }
}

impl Default for SharedLogFile {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_basic_indexing() {
        let content = "line1\nline2\nline3\n";
        let file = create_test_file(content);
        let log_file = LogFile::open(file.path()).unwrap();

        assert_eq!(log_file.line_count(), 3);
    }

    #[test]
    fn test_get_lines() {
        let content = "line1\nline2\nline3\n";
        let file = create_test_file(content);
        let log_file = LogFile::open(file.path()).unwrap();

        let lines = log_file.get_lines(0, 3).unwrap();
        assert_eq!(lines, vec!["line1", "line2", "line3"]);
    }

    #[test]
    fn test_get_lines_partial() {
        let content = "line1\nline2\nline3\nline4\nline5\n";
        let file = create_test_file(content);
        let log_file = LogFile::open(file.path()).unwrap();

        let lines = log_file.get_lines(1, 2).unwrap();
        assert_eq!(lines, vec!["line2", "line3"]);
    }

    #[test]
    fn test_search() {
        let content = "error: something failed\ninfo: all good\nerror: another failure\n";
        let file = create_test_file(content);
        let log_file = LogFile::open(file.path()).unwrap();

        let results = log_file.search("error", 100).unwrap();
        assert_eq!(results, vec![0, 2]);
    }

    #[test]
    fn test_empty_file() {
        let file = create_test_file("");
        let result = LogFile::open(file.path());
        assert!(matches!(result, Err(IndexerError::EmptyFile)));
    }

    #[test]
    fn test_binary_transfer() {
        let content = "line1\nline2\n";
        let file = create_test_file(content);
        let log_file = LogFile::open(file.path()).unwrap();

        let binary = log_file.get_lines_binary(0, 2).unwrap();
        
        // Parse the binary format
        let num_lines = u32::from_le_bytes(binary[0..4].try_into().unwrap());
        assert_eq!(num_lines, 2);
    }
}

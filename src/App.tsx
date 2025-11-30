import { useState, useCallback, useEffect } from 'react';
import { LogViewer } from './components/LogViewer';
import { QueryPanel } from './components/QueryPanel';
import { ProgressBar } from './components/ProgressBar';
import { ExamplesPanel } from './components/ExamplesPanel';
import { useLogFile } from './hooks/useLogFile';

// App icon path (served from public folder)
const appIcon = '/icons/icon.png';

// Recent files storage key
const RECENT_FILES_KEY = 'log-microscope-recent-files';
const MAX_RECENT_FILES = 7;

interface RecentFile {
  path: string;
  name: string;
  lastOpened: number;
}

function getRecentFiles(): RecentFile[] {
  try {
    const stored = localStorage.getItem(RECENT_FILES_KEY);
    return stored ? JSON.parse(stored) : [];
  } catch {
    return [];
  }
}

function addRecentFile(path: string): void {
  const name = path.split('/').pop() || path;
  const recentFiles = getRecentFiles().filter(f => f.path !== path);
  recentFiles.unshift({ path, name, lastOpened: Date.now() });
  localStorage.setItem(
    RECENT_FILES_KEY,
    JSON.stringify(recentFiles.slice(0, MAX_RECENT_FILES))
  );
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function formatNumber(num: number): string {
  return num.toLocaleString();
}

function App() {
  const {
    fileInfo,
    isLoading,
    progress,
    error,
    openFile,
    openFilePath,
    closeFile,
    search,
  } = useLogFile();

  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<number[]>([]);
  const [selectedLine, setSelectedLine] = useState<number | undefined>();
  const [isSearching, setIsSearching] = useState(false);
  const [queryPanelOpen, setQueryPanelOpen] = useState(false);
  const [currentResultIndex, setCurrentResultIndex] = useState(0);
  const [showExamples, setShowExamples] = useState(false);
  const [pendingQuery, setPendingQuery] = useState<string | null>(null);
  const [wordWrap, setWordWrap] = useState(false);
  const [showRegexHelp, setShowRegexHelp] = useState(false);
  const [recentFiles, setRecentFiles] = useState<RecentFile[]>(getRecentFiles());

  // Track when a file is opened
  useEffect(() => {
    if (fileInfo?.path) {
      addRecentFile(fileInfo.path);
      setRecentFiles(getRecentFiles());
    }
  }, [fileInfo?.path]);

  // Open a recent file
  const handleOpenRecentFile = useCallback(async (path: string) => {
    await openFilePath(path);
  }, [openFilePath]);

  // Handle search
  const handleSearch = useCallback(async () => {
    if (!searchQuery.trim() || !fileInfo) return;

    setIsSearching(true);
    try {
      const results = await search(searchQuery);
      setSearchResults(results);
      setCurrentResultIndex(0);
      if (results.length > 0) {
        setSelectedLine(results[0]);
      }
    } finally {
      setIsSearching(false);
    }
  }, [searchQuery, search, fileInfo]);

  // Navigate search results
  const navigateResults = useCallback(
    (direction: 'next' | 'prev') => {
      if (searchResults.length === 0) return;

      let newIndex = currentResultIndex;
      if (direction === 'next') {
        newIndex = (currentResultIndex + 1) % searchResults.length;
      } else {
        newIndex =
          (currentResultIndex - 1 + searchResults.length) % searchResults.length;
      }

      setCurrentResultIndex(newIndex);
      setSelectedLine(searchResults[newIndex]);
    },
    [searchResults, currentResultIndex]
  );

  // Handle keyboard shortcuts
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSearch();
      } else if (e.key === 'F3' || (e.key === 'g' && (e.metaKey || e.ctrlKey))) {
        e.preventDefault();
        if (e.shiftKey) {
          navigateResults('prev');
        } else {
          navigateResults('next');
        }
      }
    },
    [handleSearch, navigateResults]
  );

  // Clear search
  const clearSearch = useCallback(() => {
    setSearchQuery('');
    setSearchResults([]);
    setSelectedLine(undefined);
    setCurrentResultIndex(0);
  }, []);

  return (
    <div className={`app-container ${showExamples ? 'examples-open' : ''}`} onKeyDown={handleKeyDown} tabIndex={0}>
      {/* Header */}
      <header className="app-header">
        <div className="header-left">
          <h1 className="app-title">Log Microscope</h1>
          {fileInfo && (
            <div className="file-info">
              <span className="file-name" title={fileInfo.path}>
                {fileInfo.path.split('/').pop()}
              </span>
              <span className="file-stats">
                {formatBytes(fileInfo.size)} • {formatNumber(fileInfo.line_count)}{' '}
                lines • {fileInfo.format}
              </span>
            </div>
          )}
        </div>
        <div className="header-actions">
          {fileInfo && (
            <button
              className={`btn btn-icon ${wordWrap ? 'active' : ''}`}
              onClick={() => setWordWrap(!wordWrap)}
              title={wordWrap ? 'Disable word wrap' : 'Enable word wrap'}
            >
              ↩ Wrap
            </button>
          )}
          <button className="btn btn-primary" onClick={openFile} disabled={isLoading}>
            {isLoading ? 'Opening...' : 'Open File'}
          </button>
          {fileInfo && (
            <button className="btn btn-secondary" onClick={closeFile}>
              Close
            </button>
          )}
        </div>
      </header>

      {/* Search Bar */}
      {fileInfo && (
        <div className="search-bar">
          <div className="search-input-wrapper">
            <div className="search-input-container">
              <input
                type="text"
                className="search-input"
                placeholder="Search (regex supported)..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    e.preventDefault();
                    handleSearch();
                  }
                }}
              />
              {searchQuery && (
                <button className="search-clear-btn" onClick={clearSearch}>
                  ✕
                </button>
              )}
            </div>
            <button
              className={`btn btn-regex-help ${showRegexHelp ? 'active' : ''}`}
              onClick={() => setShowRegexHelp(!showRegexHelp)}
              title="Regex examples"
            >
              .*
            </button>
            {showRegexHelp && (
              <div className="regex-help-dropdown">
                <div className="regex-help-header">
                  <span>Regex Quick Reference</span>
                  <button className="regex-help-close" onClick={() => setShowRegexHelp(false)}>✕</button>
                </div>
                <div className="regex-help-content">
                  <div className="regex-help-section">
                    <div className="regex-help-title">Common Patterns</div>
                    <div className="regex-help-item" onClick={() => { setSearchQuery('(?i)error'); setShowRegexHelp(false); }}>
                      <code>(?i)error</code>
                      <span>Case-insensitive "error"</span>
                    </div>
                    <div className="regex-help-item" onClick={() => { setSearchQuery('error|warning|fatal'); setShowRegexHelp(false); }}>
                      <code>error|warning|fatal</code>
                      <span>Match any of these words</span>
                    </div>
                    <div className="regex-help-item" onClick={() => { setSearchQuery('^\\d{4}-\\d{2}-\\d{2}'); setShowRegexHelp(false); }}>
                      <code>^\d&#123;4&#125;-\d&#123;2&#125;-\d&#123;2&#125;</code>
                      <span>Lines starting with date (YYYY-MM-DD)</span>
                    </div>
                    <div className="regex-help-item" onClick={() => { setSearchQuery('\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b'); setShowRegexHelp(false); }}>
                      <code>\b\d&#123;1,3&#125;(\.\d&#123;1,3&#125;)&#123;3&#125;\b</code>
                      <span>IP addresses</span>
                    </div>
                    <div className="regex-help-item" onClick={() => { setSearchQuery('"[^"]*error[^"]*"'); setShowRegexHelp(false); }}>
                      <code>"[^"]*error[^"]*"</code>
                      <span>Error inside quotes</span>
                    </div>
                  </div>
                  <div className="regex-help-section">
                    <div className="regex-help-title">Syntax Reference</div>
                    <div className="regex-help-ref"><code>.</code> Any character</div>
                    <div className="regex-help-ref"><code>*</code> Zero or more</div>
                    <div className="regex-help-ref"><code>+</code> One or more</div>
                    <div className="regex-help-ref"><code>?</code> Optional</div>
                    <div className="regex-help-ref"><code>^</code> Start of line</div>
                    <div className="regex-help-ref"><code>$</code> End of line</div>
                    <div className="regex-help-ref"><code>\d</code> Digit [0-9]</div>
                    <div className="regex-help-ref"><code>\w</code> Word char [a-zA-Z0-9_]</div>
                    <div className="regex-help-ref"><code>\s</code> Whitespace</div>
                    <div className="regex-help-ref"><code>[abc]</code> Character class</div>
                    <div className="regex-help-ref"><code>(a|b)</code> Alternation</div>
                    <div className="regex-help-ref"><code>(?i)</code> Case insensitive</div>
                  </div>
                </div>
              </div>
            )}
          </div>
          <button
            className="btn btn-search"
            onClick={handleSearch}
            disabled={isSearching || !searchQuery.trim()}
          >
            {isSearching ? 'Searching...' : 'Search'}
          </button>
          {searchResults.length > 0 && (
            <div className="search-navigation">
              <span className="search-count">
                {currentResultIndex + 1} of {formatNumber(searchResults.length)}{' '}
                matches
              </span>
              <button
                className="btn btn-nav"
                onClick={() => navigateResults('prev')}
                title="Previous (Shift+F3)"
              >
                ↑
              </button>
              <button
                className="btn btn-nav"
                onClick={() => navigateResults('next')}
                title="Next (F3)"
              >
                ↓
              </button>
            </div>
          )}
          <button
            className={`btn btn-sql ${queryPanelOpen ? 'active' : ''}`}
            onClick={() => setQueryPanelOpen(!queryPanelOpen)}
          >
            SQL
          </button>
        </div>
      )}

      {/* Loading Progress */}
      {isLoading && progress && (
        <div className="loading-overlay">
          <ProgressBar
            phase={progress.phase}
            progress={progress.progress}
            message={progress.message}
          />
        </div>
      )}

      {/* Error Display */}
      {error && (
        <div className="error-banner">
          <span className="error-icon">⚠️</span>
          <span className="error-message">{error}</span>
          <button className="error-dismiss" onClick={() => {}}>
            ✕
          </button>
        </div>
      )}

      {/* Main Content */}
      <main className="app-main">
        {!fileInfo && !isLoading && (
          <div className="welcome-screen">
            <img src={appIcon} alt="Log Microscope" className="welcome-logo" />
            <h2>Welcome to Log Microscope</h2>
            <p>
              A high-performance log viewer for analyzing gigabyte-scale log files
            </p>
            <div className="features">
              <div className="feature">
                <span className="feature-icon">●</span>
                <span>Memory-mapped file access</span>
              </div>
              <div className="feature">
                <span className="feature-icon">●</span>
                <span>Parallel regex search</span>
              </div>
              <div className="feature">
                <span className="feature-icon">●</span>
                <span>SQL query support (DataFusion)</span>
              </div>
              <div className="feature">
                <span className="feature-icon">●</span>
                <span>Virtualized rendering (60 FPS)</span>
              </div>
            </div>
            <button className="btn btn-primary btn-large" onClick={openFile}>
              Open Log File
            </button>

            {recentFiles.length > 0 && (
              <div className="recent-files">
                <h3>Recent Files</h3>
                <ul className="recent-files-list">
                  {recentFiles.map((file, idx) => (
                    <li key={idx}>
                      <button
                        className="recent-file-btn"
                        onClick={() => handleOpenRecentFile(file.path)}
                        title={file.path}
                      >
                        <span className="recent-file-icon">📄</span>
                        <span className="recent-file-name">{file.name}</span>
                        <span className="recent-file-path">{file.path}</span>
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        {fileInfo && (
          <LogViewer
            lineCount={fileInfo.line_count}
            searchResults={searchResults}
            selectedLine={selectedLine}
            onLineClick={setSelectedLine}
            wordWrap={wordWrap}
          />
        )}
      </main>

      {/* SQL Query Panel */}
      {fileInfo && (
        <QueryPanel
          isOpen={queryPanelOpen}
          onToggle={() => setQueryPanelOpen(!queryPanelOpen)}
          onShowExamples={() => setShowExamples(!showExamples)}
          showExamplesActive={showExamples}
          onLineSelect={setSelectedLine}
          externalQuery={pendingQuery}
          onQueryConsumed={() => setPendingQuery(null)}
        />
      )}

      {/* Examples Panel - Full height on right side */}
      {fileInfo && (
        <ExamplesPanel
          isOpen={showExamples}
          onClose={() => setShowExamples(false)}
          onSelectQuery={(query) => {
            setPendingQuery(query);
            setQueryPanelOpen(true);
          }}
          onRunQuery={(query) => {
            setPendingQuery(query);
            setQueryPanelOpen(true);
            // The QueryPanel will execute when it receives the query
          }}
        />
      )}

      {/* Footer */}
      <footer className="app-footer">
        <span>
          Built with Tauri + React • Memory-mapped I/O • DataFusion SQL •
          TanStack Virtual
        </span>
      </footer>
    </div>
  );
}

export default App;

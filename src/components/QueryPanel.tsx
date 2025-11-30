import { useState, useCallback, useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';

interface QueryPanelProps {
  isOpen: boolean;
  onToggle: () => void;
  onShowExamples: () => void;
  showExamplesActive: boolean;
  onLineSelect?: (lineNumber: number) => void;
  externalQuery?: string | null;
  onQueryConsumed?: () => void;
}

interface QueryResult {
  columns: string[];
  rows: (string | number | boolean | null)[][];
  row_count: number;
}

export function QueryPanel({
  isOpen,
  onToggle,
  onShowExamples,
  showExamplesActive,
  onLineSelect,
  externalQuery,
  onQueryConsumed,
}: QueryPanelProps) {
  const [query, setQuery] = useState('SELECT * FROM logs LIMIT 100');
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  // Handle external query updates
  useEffect(() => {
    if (externalQuery) {
      setQuery(externalQuery);
      onQueryConsumed?.();
    }
  }, [externalQuery, onQueryConsumed]);

  const executeQuery = useCallback(async (queryToRun?: string) => {
    const sql = queryToRun ?? query;
    if (queryToRun) {
      setQuery(queryToRun);
    }
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const queryResult = await invoke<QueryResult>('execute_sql', { query: sql });
      setResult(queryResult);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [query]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        executeQuery();
      }
    },
    [executeQuery]
  );

  // Find line_number column index if it exists
  const lineNumberColIndex = result?.columns.findIndex(
    (col) => col.toLowerCase() === 'line_number'
  ) ?? -1;

  const handleRowClick = useCallback(
    (row: (string | number | boolean | null)[]) => {
      if (lineNumberColIndex >= 0 && onLineSelect) {
        const lineNum = row[lineNumberColIndex];
        if (typeof lineNum === 'number') {
          // line_number is 1-indexed, but our viewer uses 0-indexed
          onLineSelect(lineNum - 1);
        }
      }
    },
    [lineNumberColIndex, onLineSelect]
  );

  if (!isOpen) {
    return (
      <div className="query-panel-collapsed" onClick={onToggle}>
        <span className="query-panel-toggle">▲ SQL Query Panel</span>
      </div>
    );
  }

  return (
    <div className="query-panel">
      <div className="query-panel-header">
        <span className="query-panel-title">SQL Query Panel</span>
        <div className="query-panel-actions">
          <button
            className={`query-examples-btn ${showExamplesActive ? 'active' : ''}`}
            onClick={onShowExamples}
          >
            📚 Examples
          </button>
          <button className="query-panel-toggle-btn" onClick={onToggle}>
            ▼ Collapse
          </button>
        </div>
      </div>

      <div className="query-panel-body">
        <div className="query-input-container">
          <textarea
            className="query-input"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Enter SQL query (Cmd/Ctrl + Enter to execute)"
            rows={3}
          />
          <button
            className="query-execute-btn"
            onClick={() => executeQuery()}
            disabled={loading || !query.trim()}
          >
            {loading ? 'Running...' : 'Execute (⌘↵)'}
          </button>
        </div>

        <div className="query-help-inline">
          <span><strong>Table:</strong> <code>logs</code> (line_number, line)</span>
          <span><strong>UDFs:</strong> <code>regex_match(col, 'pattern')</code>, <code>json_extract(col, 'key')</code></span>
        </div>

        {error && (
          <div className="query-error">
            <strong>Error:</strong> {error}
          </div>
        )}

        {result && (
          <div className="query-results">
            <div className="query-results-header">
              <span>
                {result.row_count} row{result.row_count !== 1 ? 's' : ''} returned
                {lineNumberColIndex >= 0 && (
                  <span className="query-results-hint"> • Click a row to jump to that line</span>
                )}
              </span>
            </div>
            <div className="query-results-table-container">
              <table className="query-results-table">
                <thead>
                  <tr>
                    {result.columns.map((col, idx) => (
                      <th key={idx}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.rows.map((row, rowIdx) => (
                    <tr
                      key={rowIdx}
                      className={lineNumberColIndex >= 0 ? 'clickable' : ''}
                      onClick={() => handleRowClick(row)}
                    >
                      {row.map((cell, cellIdx) => (
                        <td key={cellIdx} title={cell !== null ? String(cell) : undefined}>
                          {cell === null
                            ? 'NULL'
                            : typeof cell === 'object'
                            ? JSON.stringify(cell)
                            : String(cell)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default QueryPanel;

import { useState, useCallback } from 'react';

interface ExamplesPanelProps {
  isOpen: boolean;
  onClose: () => void;
  onSelectQuery: (query: string) => void;
  onRunQuery: (query: string) => void;
  isLoading?: boolean;
}

const SQL_EXAMPLES = [
  {
    name: 'Basic Selection',
    description: 'Get first 100 log lines',
    query: 'SELECT * FROM logs LIMIT 100',
  },
  {
    name: 'Filter by Pattern',
    description: 'Find lines containing specific text',
    query: "SELECT * FROM logs WHERE line LIKE '%ERROR%' LIMIT 50",
  },
  {
    name: 'Regex Search',
    description: 'Use regex to find patterns',
    query: "SELECT * FROM logs WHERE regex_match(line, '(?i)(error|exception)') LIMIT 50",
  },
  {
    name: 'Count by Level',
    description: 'Count occurrences of each log level',
    query: `SELECT 
  CASE 
    WHEN regex_match(line, '(?i)\\bERROR\\b') THEN 'ERROR'
    WHEN regex_match(line, '(?i)\\bWARN\\b') THEN 'WARN'
    WHEN regex_match(line, '(?i)\\bINFO\\b') THEN 'INFO'
    WHEN regex_match(line, '(?i)\\bDEBUG\\b') THEN 'DEBUG'
    ELSE 'OTHER'
  END as level,
  COUNT(*) as count
FROM logs
GROUP BY level
ORDER BY count DESC`,
  },
  {
    name: 'JSON Level Extract',
    description: 'Extract and count JSON log levels',
    query: `SELECT json_extract(line, 'level') as level, COUNT(*) as count
FROM logs
WHERE json_extract(line, 'level') IS NOT NULL
GROUP BY level`,
  },
  {
    name: 'Recent Errors',
    description: 'Get recent error lines with line numbers',
    query: "SELECT line_number, line FROM logs WHERE regex_match(line, '(?i)error') ORDER BY line_number DESC LIMIT 20",
  },
  {
    name: 'Time Range (JSON)',
    description: 'Filter by timestamp in JSON logs',
    query: `SELECT * FROM logs 
WHERE json_extract(line, 'timestamp') >= '2024-01-01'
LIMIT 50`,
  },
  {
    name: 'Specific Field',
    description: 'Extract a specific JSON field',
    query: "SELECT line_number, json_extract(line, 'message') as message FROM logs LIMIT 50",
  },
];

export function ExamplesPanel({
  isOpen,
  onClose,
  onSelectQuery,
  onRunQuery,
  isLoading = false,
}: ExamplesPanelProps) {
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);

  const copyToClipboard = useCallback(async (text: string, index: number) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedIndex(index);
      setTimeout(() => setCopiedIndex(null), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  }, []);

  return (
    <div className={`examples-panel ${isOpen ? 'open' : ''}`}>
      <div className="examples-panel-header">
        <h3>📚 SQL Examples</h3>
        <button className="examples-panel-close" onClick={onClose}>
          ✕
        </button>
      </div>
      <div className="examples-panel-list">
        {SQL_EXAMPLES.map((example, idx) => (
          <div key={idx} className="examples-panel-item">
            <div className="examples-panel-item-header">
              <div className="examples-panel-item-name">{example.name}</div>
              <div className="examples-panel-item-actions">
                <button
                  className="examples-panel-action-btn"
                  onClick={() => copyToClipboard(example.query, idx)}
                  title="Copy to clipboard"
                >
                  {copiedIndex === idx ? '✓' : '📋'}
                </button>
                <button
                  className="examples-panel-action-btn run"
                  onClick={() => onRunQuery(example.query)}
                  title="Run this query"
                  disabled={isLoading}
                >
                  ▶
                </button>
              </div>
            </div>
            <div className="examples-panel-item-desc">{example.description}</div>
            <pre
              className="examples-panel-item-code"
              onClick={() => onSelectQuery(example.query)}
              title="Click to load into editor"
            >
              {example.query}
            </pre>
          </div>
        ))}
      </div>
    </div>
  );
}

export default ExamplesPanel;

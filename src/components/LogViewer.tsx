import { useCallback, useEffect, useRef, useState } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { invoke } from '@tauri-apps/api/core';

interface LogViewerProps {
  lineCount: number;
  searchResults?: number[];
  selectedLine?: number;
  onLineClick?: (lineNumber: number) => void;
  wordWrap?: boolean;
}

const LINE_HEIGHT_ESTIMATE = 24;
const OVERSCAN = 20;
const BATCH_SIZE = 100;

interface LineCache {
  [key: number]: string;
}

export function LogViewer({
  lineCount,
  searchResults,
  selectedLine,
  onLineClick,
  wordWrap = false,
}: LogViewerProps) {
  const parentRef = useRef<HTMLDivElement>(null);
  const [lineCache, setLineCache] = useState<LineCache>({});
  const [loading, setLoading] = useState<Set<number>>(new Set());
  const searchResultsSet = new Set(searchResults || []);

  // Fetch lines from the Rust backend
  const fetchLines = useCallback(
    async (start: number, count: number) => {
      // Skip if already loading these lines
      const linesToFetch: number[] = [];
      for (let i = start; i < start + count && i < lineCount; i++) {
        if (lineCache[i] === undefined && !loading.has(i)) {
          linesToFetch.push(i);
        }
      }

      if (linesToFetch.length === 0) return;

      const fetchStart = linesToFetch[0];
      const fetchCount = linesToFetch[linesToFetch.length - 1] - fetchStart + 1;

      // Mark as loading
      setLoading((prev) => {
        const next = new Set(prev);
        for (let i = fetchStart; i < fetchStart + fetchCount; i++) {
          next.add(i);
        }
        return next;
      });

      try {
        const lines = await invoke<string[]>('get_lines', {
          start: fetchStart,
          count: fetchCount,
        });

        // Update cache
        setLineCache((prev) => {
          const next = { ...prev };
          lines.forEach((line, idx) => {
            next[fetchStart + idx] = line;
          });
          return next;
        });
      } catch (error) {
        console.error('Failed to fetch lines:', error);
      } finally {
        // Remove from loading
        setLoading((prev) => {
          const next = new Set(prev);
          for (let i = fetchStart; i < fetchStart + fetchCount; i++) {
            next.delete(i);
          }
          return next;
        });
      }
    },
    [lineCache, lineCount, loading]
  );

  // Virtual list setup
  const virtualizer = useVirtualizer({
    count: lineCount,
    getScrollElement: () => parentRef.current,
    estimateSize: () => LINE_HEIGHT_ESTIMATE,
    overscan: OVERSCAN,
  });

  const virtualItems = virtualizer.getVirtualItems();

  // Fetch visible lines when they change
  useEffect(() => {
    if (virtualItems.length === 0) return;

    const start = virtualItems[0].index;
    const end = virtualItems[virtualItems.length - 1].index;
    const count = end - start + 1;

    fetchLines(start, Math.min(count + OVERSCAN * 2, BATCH_SIZE));
  }, [virtualItems, fetchLines]);

  // Clear cache when file changes
  useEffect(() => {
    setLineCache({});
  }, [lineCount]);

  // Scroll to selected line
  useEffect(() => {
    if (selectedLine !== undefined && selectedLine >= 0 && selectedLine < lineCount) {
      virtualizer.scrollToIndex(selectedLine, { align: 'center' });
    }
  }, [selectedLine, lineCount, virtualizer]);

  // Highlight search terms in a line
  const highlightLine = useCallback((line: string, isSearchResult: boolean) => {
    if (!isSearchResult) return line;
    return line; // Could add highlighting logic here
  }, []);

  // Classify log line for coloring - smarter detection for log levels
  const getLineClass = useCallback((line: string): string => {
    // Common log level patterns - look for level indicators at typical positions
    // Matches: [ERROR], "level":"error", ERROR:, | ERROR |, etc.
    const levelPatterns = {
      error: /(?:^|[\[\s"':=|])(?:ERROR|FATAL|CRITICAL|SEVERE|EMERGENCY|ALERT)(?:[\]\s"':=|,]|$)/i,
      warning: /(?:^|[\[\s"':=|])(?:WARN|WARNING)(?:[\]\s"':=|,]|$)/i,
      debug: /(?:^|[\[\s"':=|])(?:DEBUG|TRACE|VERBOSE)(?:[\]\s"':=|,]|$)/i,
      info: /(?:^|[\[\s"':=|])(?:INFO|NOTICE)(?:[\]\s"':=|,]|$)/i,
    };

    // Also check for JSON "level" field specifically
    const jsonLevelMatch = line.match(/"level"\s*:\s*"([^"]+)"/i);
    if (jsonLevelMatch) {
      const level = jsonLevelMatch[1].toLowerCase();
      if (['error', 'fatal', 'critical', 'severe', 'emergency', 'alert'].includes(level)) {
        return 'log-line-error';
      }
      if (['warn', 'warning'].includes(level)) {
        return 'log-line-warning';
      }
      if (['debug', 'trace', 'verbose'].includes(level)) {
        return 'log-line-debug';
      }
      if (['info', 'notice'].includes(level)) {
        return 'log-line-info';
      }
    }

    // Check patterns for non-JSON logs
    if (levelPatterns.error.test(line)) return 'log-line-error';
    if (levelPatterns.warning.test(line)) return 'log-line-warning';
    if (levelPatterns.debug.test(line)) return 'log-line-debug';
    if (levelPatterns.info.test(line)) return 'log-line-info';

    return '';
  }, []);

  return (
    <div className={`log-viewer-container ${wordWrap ? 'word-wrap' : ''}`}>
      <div ref={parentRef} className="log-viewer-scroll">
        <div
          style={{
            height: `${virtualizer.getTotalSize()}px`,
            width: '100%',
            position: 'relative',
          }}
        >
          {virtualItems.map((virtualRow) => {
            const lineIndex = virtualRow.index;
            const line = lineCache[lineIndex];
            const isSearchResult = searchResultsSet.has(lineIndex);
            const isSelected = selectedLine === lineIndex;
            const isLoading = loading.has(lineIndex);

            return (
              <div
                key={virtualRow.key}
                ref={virtualizer.measureElement}
                data-index={virtualRow.index}
                className={`log-line ${getLineClass(line || '')} ${
                  isSearchResult ? 'log-line-search-result' : ''
                } ${isSelected ? 'log-line-selected' : ''}`}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  transform: `translateY(${virtualRow.start}px)`,
                }}
                onClick={() => onLineClick?.(lineIndex)}
              >
                <span className="line-number">{lineIndex + 1}</span>
                <span className="line-content">
                  {isLoading ? (
                    <span className="loading-placeholder">Loading...</span>
                  ) : line !== undefined ? (
                    highlightLine(line, isSearchResult)
                  ) : (
                    <span className="loading-placeholder">...</span>
                  )}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

export default LogViewer;

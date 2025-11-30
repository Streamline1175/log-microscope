import { useState, useCallback, useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { open } from '@tauri-apps/plugin-dialog';

export interface FileInfo {
  path: string;
  size: number;
  line_count: number;
  format: string;
}

export interface IndexProgress {
  phase: string;
  progress: number;
  message: string;
}

export interface UseLogFileReturn {
  fileInfo: FileInfo | null;
  isLoading: boolean;
  progress: IndexProgress | null;
  error: string | null;
  openFile: () => Promise<void>;
  openFilePath: (path: string) => Promise<void>;
  closeFile: () => Promise<void>;
  search: (pattern: string, maxResults?: number) => Promise<number[]>;
  getLines: (start: number, count: number) => Promise<string[]>;
}

export function useLogFile(): UseLogFileReturn {
  const [fileInfo, setFileInfo] = useState<FileInfo | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [progress, setProgress] = useState<IndexProgress | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Listen for progress events
  useEffect(() => {
    const unlisten = listen<IndexProgress>('index-progress', (event) => {
      setProgress(event.payload);
    });

    return () => {
      unlisten.then((fn) => fn());
    };
  }, []);

  // Open file dialog and load file
  const openFile = useCallback(async () => {
    try {
      const selected = await open({
        multiple: false,
        filters: [
          {
            name: 'Log Files',
            extensions: ['log', 'txt', 'json', 'ndjson', 'csv', '*'],
          },
        ],
      });

      if (!selected) return;

      setIsLoading(true);
      setError(null);
      setProgress(null);

      const path = typeof selected === 'string' ? selected : selected;
      const info = await invoke<FileInfo>('open_file', { path });
      setFileInfo(info);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      console.error('Failed to open file:', err);
    } finally {
      setIsLoading(false);
      setProgress(null);
    }
  }, []);

  // Open a file by path directly
  const openFilePath = useCallback(async (path: string) => {
    try {
      setIsLoading(true);
      setError(null);
      setProgress(null);

      const info = await invoke<FileInfo>('open_file', { path });
      setFileInfo(info);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      console.error('Failed to open file:', err);
    } finally {
      setIsLoading(false);
      setProgress(null);
    }
  }, []);

  // Close the current file
  const closeFile = useCallback(async () => {
    try {
      await invoke('close_file');
      setFileInfo(null);
      setError(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      console.error('Failed to close file:', err);
    }
  }, []);

  // Search for a pattern in the file
  const search = useCallback(
    async (pattern: string, maxResults: number = 1000): Promise<number[]> => {
      if (!fileInfo) return [];

      try {
        const results = await invoke<number[]>('search', {
          pattern,
          maxResults,
        });
        return results;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        console.error('Search failed:', err);
        return [];
      }
    },
    [fileInfo]
  );

  // Get lines from the file
  const getLines = useCallback(
    async (start: number, count: number): Promise<string[]> => {
      if (!fileInfo) return [];

      try {
        const lines = await invoke<string[]>('get_lines', { start, count });
        return lines;
      } catch (err) {
        console.error('Failed to get lines:', err);
        return [];
      }
    },
    [fileInfo]
  );

  return {
    fileInfo,
    isLoading,
    progress,
    error,
    openFile,
    openFilePath,
    closeFile,
    search,
    getLines,
  };
}

export default useLogFile;

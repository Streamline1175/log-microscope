interface ProgressBarProps {
  phase: string;
  progress: number;
  message: string;
}

export function ProgressBar({ phase, progress, message }: ProgressBarProps) {
  const percentage = Math.round(progress * 100);

  return (
    <div className="progress-bar-container">
      <div className="progress-bar-header">
        <span className="progress-phase">{phase}</span>
        <span className="progress-percentage">{percentage}%</span>
      </div>
      <div className="progress-bar-track">
        <div
          className="progress-bar-fill"
          style={{ width: `${percentage}%` }}
        />
      </div>
      <div className="progress-message">{message}</div>
    </div>
  );
}

export default ProgressBar;

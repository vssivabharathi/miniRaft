import React from 'react';
import { FileTerminal, Server } from 'lucide-react';
import type { NodeLogSummary } from '../../types';

interface Props {
  logs: NodeLogSummary[];
  isLoading?: boolean;
}

const LogReplicationPanelComponent = ({ logs, isLoading }: Props) => {
  if (isLoading) {
    return (
      <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm flex flex-col h-[350px]">
        <div className="flex justify-between items-start mb-4 shrink-0">
          <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider flex items-center gap-2">
            <FileTerminal className="w-4 h-4 text-primary" />
            Live Raft Log
          </h2>
        </div>
        <div className="flex-1 flex items-center justify-center text-sm text-text-muted">
          Loading cluster logs...
        </div>
      </div>
    );
  }

  const nodes = logs ?? [];

  if (nodes.length === 0) {
    return (
      <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm flex flex-col h-[350px]">
        <div className="flex justify-between items-start mb-4 shrink-0">
          <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider flex items-center gap-2">
            <FileTerminal className="w-4 h-4 text-primary" />
            Live Raft Log
          </h2>
        </div>
        <div className="flex-1 flex items-center justify-center text-sm text-text-muted">
          No cluster nodes available.
        </div>
      </div>
    );
  }

  const leaderLog = nodes.find(n => n.state === 'LEADER') || nodes[0];
  const entries = leaderLog.entries ?? [];

  // Reverse sort entries so the newest commands are at the top
  const sortedEntries = [...entries].sort((a, b) => b.index - a.index);

  return (
    <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm flex flex-col h-[350px]">
      <div className="flex justify-between items-start mb-4 shrink-0">
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider flex items-center gap-2">
          <FileTerminal className="w-4 h-4 text-primary" />
          Live Raft Log Replication
        </h2>
        <div className="flex items-center gap-2">
          <div className="text-xs font-mono text-text-muted bg-background px-2 py-1 border border-border-subtle rounded flex items-center gap-1.5">
            <Server className="w-3.5 h-3.5" />
            Tracking Node {leaderLog.node_id} ({leaderLog.state})
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-x-auto border border-border-subtle rounded custom-scrollbar bg-background">
        <table className="w-full text-left text-sm whitespace-nowrap min-w-[600px]">
          <thead className="bg-panel sticky top-0 border-b border-border-subtle shadow-sm z-10">
            <tr>
              <th className="px-4 py-2 font-medium text-text-muted text-xs uppercase tracking-wider w-24">Index</th>
              <th className="px-4 py-2 font-medium text-text-muted text-xs uppercase tracking-wider w-24">Term</th>
              <th className="px-4 py-2 font-medium text-text-muted text-xs uppercase tracking-wider">Command Payload</th>
              <th className="px-4 py-2 font-medium text-text-muted text-xs uppercase tracking-wider w-32 text-right">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-subtle">
            {sortedEntries.length === 0 && (
              <tr>
                <td colSpan={4} className="px-4 py-8 text-center text-text-muted text-sm italic">
                  Log is currently empty (or entirely compacted)
                </td>
              </tr>
            )}
            {sortedEntries.map(entry => (
              <tr 
                key={entry.index}
                className="transition-colors hover:bg-border-subtle/50 bg-panel"
              >
                <td className="px-4 py-2.5 font-mono text-xs text-text-primary">{entry.index}</td>
                <td className="px-4 py-2.5 font-mono text-xs text-text-muted">{entry.term}</td>
                <td className="px-4 py-2.5 font-mono text-xs text-primary font-medium">
                  {entry.command}
                </td>
                <td className="px-4 py-2.5 text-right">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider border ${
                    entry.committed 
                      ? 'bg-success/10 text-success border-success/20' 
                      : 'bg-warning/10 text-warning border-warning/20'
                  }`}>
                    {entry.committed ? 'COMMITTED' : 'REPLICATING'}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default React.memo(LogReplicationPanelComponent);

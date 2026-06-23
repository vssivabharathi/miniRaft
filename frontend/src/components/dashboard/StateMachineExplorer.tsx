import React from 'react';
import { Database, Server } from 'lucide-react';
import type { NodeStateMachineSummary } from '../../types';

interface Props {
  stateMachineData: NodeStateMachineSummary[];
  isLoading?: boolean;
}

const StateMachineExplorer = ({ stateMachineData, isLoading }: Props) => {
  if (isLoading) {
    return (
      <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm h-[350px] flex flex-col">
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider mb-4 flex items-center gap-2">
          <Database className="w-4 h-4 text-primary" />
          Replicated State Machine
        </h2>
        <div className="flex-1 flex items-center justify-center text-sm text-text-muted">
          Loading state machine data...
        </div>
      </div>
    );
  }

  const nodes = stateMachineData ?? [];

  if (nodes.length === 0) {
    return (
      <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm h-[350px] flex flex-col">
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider mb-4 flex items-center gap-2">
          <Database className="w-4 h-4 text-primary" />
          Replicated State Machine
        </h2>
        <div className="flex-1 flex items-center justify-center text-sm text-text-muted">
          No cluster nodes available.
        </div>
      </div>
    );
  }

  // Render actual data whenever nodes.length > 0 even if no leader exists.
  const leaderState = nodes[0];
  const kv = leaderState.kv ?? {};
  const keys = Object.keys(kv).sort();

  return (
    <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm flex flex-col h-[350px]">
      <div className="flex justify-between items-start mb-4 shrink-0">
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider flex items-center gap-2">
          <Database className="w-4 h-4 text-primary" />
          Replicated State Machine (KV Store)
        </h2>
        <div className="flex items-center gap-2">
          <div className="text-xs font-mono text-text-muted bg-background px-2 py-1 border border-border-subtle rounded flex items-center gap-1.5">
            <Server className="w-3.5 h-3.5" />
            Global State View
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto custom-scrollbar bg-background border border-border-subtle rounded p-4">
        {keys.length === 0 ? (
          <div className="h-full flex items-center justify-center text-sm text-text-muted italic">
            State Machine is currently empty.
            <br/> Use Chaos Controls to submit SET commands.
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            {keys.map(k => (
              <div key={k} className="bg-panel border border-border-subtle rounded p-3 shadow-sm flex items-center justify-between group hover:border-primary/50 transition-colors">
                <span className="font-mono text-sm font-bold text-text-primary">{k}</span>
                <span className="font-mono text-xs text-success bg-success/10 px-2 py-1 rounded">
                  {kv[k]}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
      <div className="mt-4 text-[10px] text-text-muted uppercase tracking-wider text-center">
        Every node independently applies committed log entries to achieve identical state
      </div>
    </div>
  );
};

export default React.memo(StateMachineExplorer);

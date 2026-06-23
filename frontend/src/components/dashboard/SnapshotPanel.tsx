import React from 'react';
import { Camera, HardDrive, FileArchive, Package, Percent, Server } from 'lucide-react';
import type { NodeSnapshotSummary } from '../../types';

interface Props {
  snapshots: NodeSnapshotSummary[];
  snapshotStats: { saved: number, count: number };
  isLoading?: boolean;
}

const SnapshotPanelComponent = ({ snapshots, snapshotStats, isLoading }: Props) => {
  if (isLoading) {
    return (
      <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm flex flex-col h-[350px]">
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider mb-6 flex items-center gap-2">
          <Camera className="w-4 h-4 text-primary" />
          Snapshot & Compaction Engine
        </h2>
        <div className="flex-1 flex items-center justify-center text-sm text-text-muted">
          Loading snapshot data...
        </div>
      </div>
    );
  }

  const nodes = snapshots ?? [];

  if (nodes.length === 0) {
    return (
      <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm flex flex-col h-[350px]">
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider mb-6 flex items-center gap-2">
          <Camera className="w-4 h-4 text-primary" />
          Snapshot & Compaction Engine
        </h2>
        <div className="flex-1 flex items-center justify-center text-sm text-text-muted">
          No snapshot data available.
        </div>
      </div>
    );
  }

  const leaderSnap = nodes[0];

  const compactedIndex = leaderSnap.lastIncludedIndex;
  const logLength = leaderSnap.physicalLogLength;
  const logicalLength = compactedIndex + logLength;
  const storageSavedPercent = logicalLength > 0 ? ((compactedIndex / logicalLength) * 100).toFixed(1) : "0.0";

  // Generate visual array blocks
  const visualBlocks = [];
  if (compactedIndex > 0) {
    visualBlocks.push(
      <div key="snap" className="bg-warning/20 border-2 border-warning text-warning px-3 py-1.5 rounded text-xs font-bold font-mono shadow-sm flex items-center gap-1">
        <FileArchive className="w-3 h-3" />
        SNAPSHOT_UP_TO_{compactedIndex}
      </div>
    );
  }

  // Show up to 15 physical log blocks
  const displayCount = Math.min(logLength, 15);
  for (let i = 1; i <= displayCount; i++) {
    visualBlocks.push(
      <div key={`idx-${compactedIndex + i}`} className="bg-panel border border-border-subtle text-text-primary px-3 py-1.5 rounded text-xs font-bold font-mono shadow-sm">
        [{compactedIndex + i}]
      </div>
    );
  }
  if (logLength > 15) {
    visualBlocks.push(
      <div key="ellipsis" className="text-text-muted px-2 font-bold tracking-widest">
        ...
      </div>
    );
  }

  return (
    <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm">
      <div className="flex justify-between items-start mb-6">
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider flex items-center gap-2">
          <Camera className="w-4 h-4 text-primary" />
          Snapshot & Compaction Engine
        </h2>
        <div className="text-xs font-mono text-text-muted bg-background px-2 py-1 border border-border-subtle rounded flex items-center gap-1.5">
          <Server className="w-3.5 h-3.5" />
          Tracking Node {leaderSnap.node_id}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
        {/* Left Side: Dense Metric Cards */}
        <div className="grid grid-cols-2 gap-4">
          <div className="border border-border-subtle p-3 rounded-md bg-background flex flex-col justify-between">
            <div className="flex items-center gap-2 text-text-muted mb-2">
              <Package className="w-4 h-4 text-primary" />
              <span className="text-xs font-medium uppercase">Last Included Index</span>
            </div>
            <span className="font-mono text-xl text-text-primary font-bold">{compactedIndex}</span>
          </div>

          <div className="border border-border-subtle p-3 rounded-md bg-background flex flex-col justify-between">
            <div className="flex items-center gap-2 text-text-muted mb-2">
              <HardDrive className="w-4 h-4 text-success" />
              <span className="text-xs font-medium uppercase">Physical Array Length</span>
            </div>
            <span className="font-mono text-xl text-text-primary font-bold">{logLength}</span>
          </div>

          <div className="border border-border-subtle p-3 rounded-md bg-background flex flex-col justify-between">
            <div className="flex items-center gap-2 text-text-muted mb-2">
              <FileArchive className="w-4 h-4 text-warning" />
              <span className="text-xs font-medium uppercase">Snapshot Triggers</span>
            </div>
            <span className="font-mono text-xl text-text-primary font-bold">{snapshotStats.count}</span>
          </div>

          <div className="border border-border-subtle p-3 rounded-md bg-background flex flex-col justify-between">
            <div className="flex items-center gap-2 text-text-muted mb-2">
              <Percent className="w-4 h-4 text-danger" />
              <span className="text-xs font-medium uppercase">Memory Saved</span>
            </div>
            <span className="font-mono text-xl text-text-primary font-bold">{storageSavedPercent}%</span>
          </div>
        </div>

        {/* Right Side: Progress Bars */}
        <div className="flex flex-col justify-center gap-6 border-l border-border-subtle pl-8">
          <div className="relative">
            <div className="flex justify-between text-xs font-medium text-text-primary mb-2 uppercase tracking-wider">
              <span>Logical State Machine</span>
              <span className="font-mono">{logicalLength} cmds</span>
            </div>
            <div className="w-full bg-border-subtle h-2.5 rounded-full overflow-hidden">
              <div className="bg-primary h-full w-full opacity-80"></div>
            </div>
          </div>

          <div className="relative">
            <div className="flex justify-between text-xs font-medium text-text-primary mb-2 uppercase tracking-wider">
              <span>Physical Memory Allocation</span>
              <span className="font-mono text-success">{logLength} cmds</span>
            </div>
            <div className="w-full bg-border-subtle h-2.5 rounded-full overflow-hidden relative flex">
              <div 
                className="bg-background h-full opacity-50 striped-bg transition-all duration-500" 
                style={{ width: `${storageSavedPercent}%` }}
                title="Compacted Space"
              ></div>
              <div 
                className="bg-success h-full transition-all duration-500" 
                style={{ width: `${100 - parseFloat(storageSavedPercent)}%` }}
              ></div>
            </div>
            <div className="mt-2 text-[10px] text-text-muted text-right">
              {storageSavedPercent}% overhead reduced via snapshotting
            </div>
          </div>
        </div>
      </div>

      {/* Visual Timeline Explorer */}
      <div className="border-t border-border-subtle pt-6">
        <div className="text-xs font-semibold text-text-muted uppercase tracking-wider mb-4">
          Physical Array Layout in Memory
        </div>
        <div className="bg-background border border-border-subtle rounded p-4 flex flex-wrap gap-2 items-center">
          {visualBlocks.length > 0 ? visualBlocks : <span className="text-sm text-text-muted font-mono">[ Empty Log ]</span>}
        </div>
        <div className="mt-3 text-[11px] text-text-muted flex gap-4">
          <span className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-warning"></div> Snapshot binary blob</span>
          <span className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-panel border border-border-subtle"></div> Physical LogEntry structs</span>
        </div>
      </div>

      <style>{`
        .striped-bg {
          background-image: repeating-linear-gradient(45deg, transparent, transparent 5px, var(--border-color) 5px, var(--border-color) 10px);
        }
      `}</style>
    </div>
  );
};

export default React.memo(SnapshotPanelComponent);

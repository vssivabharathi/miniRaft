import React from 'react';
import type { ClusterSummary, MetricsSnapshot } from '../../types';
import { Server, Activity, Database, HeartPulse } from 'lucide-react';

interface Props {
  cluster: ClusterSummary;
  metrics: MetricsSnapshot[];
}

const NodeCardsComponent = ({ cluster, metrics }: Props) => {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
      {cluster.nodes.map(node => {
        const m = metrics.find(m => m.NodeID === node.id);
        
        let stateColor = 'text-text-muted';
        let borderColor = 'border-border-subtle';
        if (node.state === 'LEADER') {
          stateColor = 'text-success';
          borderColor = 'border-success ring-1 ring-success/30';
        } else if (node.state === 'FOLLOWER') {
          stateColor = 'text-primary';
        } else if (node.state === 'CANDIDATE') {
          stateColor = 'text-warning';
        } else if (node.state === 'DEAD') {
          stateColor = 'text-danger';
          borderColor = 'border-danger/50 opacity-60 grayscale';
        }

        return (
          <div key={node.id} className={`bg-panel rounded-md p-4 shadow-sm border ${borderColor}`}>
            <div className="flex justify-between items-start mb-4">
              <div className="flex items-center gap-2">
                <div className="bg-background border border-border-subtle p-1.5 rounded">
                  <Server className="w-4 h-4 text-text-primary" />
                </div>
                <div>
                  <h3 className="font-semibold text-text-primary text-sm">Node {node.id}</h3>
                  <div className={`text-[10px] font-bold uppercase tracking-wider ${stateColor}`}>
                    {node.state}
                  </div>
                </div>
              </div>
            </div>

            <div className="space-y-3">
              <div className="flex justify-between items-center text-xs">
                <span className="text-text-muted">Term</span>
                <span className="font-mono text-text-primary">{node.term}</span>
              </div>
              
              <div className="flex justify-between items-center text-xs">
                <span className="text-text-muted flex items-center gap-1">
                  <Database className="w-3 h-3" /> Commit Index
                </span>
                <span className="font-mono text-text-primary">{node.commitIndex}</span>
              </div>
              
              <div className="flex justify-between items-center text-xs">
                <span className="text-text-muted flex items-center gap-1">
                  <Database className="w-3 h-3" /> Log Length
                </span>
                <span className="font-mono text-text-primary">{node.logLength}</span>
              </div>
              
              <div className="pt-2 border-t border-border-subtle space-y-2">
                <div className="flex justify-between items-center text-[10px]">
                  <span className="text-text-muted flex items-center gap-1">
                    <Activity className="w-3 h-3" /> RPCs
                  </span>
                  <span className="font-mono text-text-secondary">
                    {m ? `${m.RPCSent} ↑ / ${m.RPCReceived} ↓` : '-'}
                  </span>
                </div>
                <div className="flex justify-between items-center text-[10px]">
                  <span className="text-text-muted flex items-center gap-1">
                    <HeartPulse className="w-3 h-3" /> Heartbeats
                  </span>
                  <span className="font-mono text-text-secondary">
                    {m ? `${m.HeartbeatsSent} ↑ / ${m.HeartbeatsReceived} ↓` : '-'}
                  </span>
                </div>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
};

export default React.memo(NodeCardsComponent);

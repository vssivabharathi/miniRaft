import { Server, Activity, Users } from 'lucide-react';
import type { ClusterSummary } from '../../types';

interface Props {
  cluster?: ClusterSummary;
}

export default function Navbar({ cluster }: Props) {
  const aliveNodes = cluster?.nodes.filter(n => n.state !== 'DEAD').length || 0;
  const totalNodes = cluster?.nodes.length || 0;
  const healthy = aliveNodes > totalNodes / 2;
  const leader = cluster?.nodes.find(n => n.state === 'LEADER');

  return (
    <header className="h-[56px] shrink-0 bg-panel border-b border-border-subtle flex items-center justify-between px-6 z-50">
      <div className="flex items-center gap-6">
        <h1 className="text-lg font-semibold tracking-tight text-text-primary flex items-center gap-3">
          <div className="bg-primary text-white p-1 rounded">
            <Server className="w-4 h-4" />
          </div>
          MiniRaft Console
        </h1>

        {cluster && (
          <div className="hidden md:flex items-center gap-4 text-sm">
            <div className="flex items-center gap-1.5 border-l border-border-subtle pl-4">
              <span className={`relative flex h-2.5 w-2.5`}>
                {healthy && <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-success opacity-75"></span>}
                <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${healthy ? 'bg-success' : 'bg-danger'}`}></span>
              </span>
              <span className="text-text-muted font-medium">{healthy ? 'Cluster Healthy' : 'Cluster Degraded'}</span>
            </div>
            
            <div className="flex items-center gap-1.5 text-text-muted border-l border-border-subtle pl-4">
              <Activity className="w-4 h-4 text-primary" />
              <span>Leader Node {leader?.id || 'None'} (Term {leader?.term || cluster.term})</span>
            </div>
            
            <div className="flex items-center gap-1.5 text-text-muted border-l border-border-subtle pl-4">
              <Users className="w-4 h-4 text-warning" />
              <span>{aliveNodes}/{totalNodes} Alive</span>
            </div>
          </div>
        )}
      </div>

      <div className="flex items-center gap-4">
        <div className="text-xs text-text-muted hidden lg:block">
          Auto-refreshing every 1s
        </div>
      </div>
    </header>
  );
}

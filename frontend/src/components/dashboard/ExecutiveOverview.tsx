import type { ClusterSummary, MetricsSnapshot } from '../../types';
import { Activity, Server, Database, Hash } from 'lucide-react';

interface Props {
  cluster: ClusterSummary;
  metrics: MetricsSnapshot[];
}

export default function ExecutiveOverview({ cluster, metrics }: Props) {
  const aliveNodes = cluster.nodes.filter(n => n.state !== 'DEAD').length;
  const totalNodes = cluster.nodes.length;
  
  // To avoid double-counting commands (since every node applies them), we can use the leader's metrics,
  // or just average them. Let's use the leader's metrics if available, otherwise max.
  const leaderMetrics = metrics.find(m => m.NodeID === cluster.leader) || 
                        metrics.reduce((max, m) => m.CommandsCommitted > (max?.CommandsCommitted || 0) ? m : max, metrics[0]);

  const leaderNode = cluster.nodes.find(n => n.id === cluster.leader);

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      <div className="bg-slate-800 rounded-xl border border-slate-700 p-5 shadow-lg flex items-start gap-4 transition-transform hover:-translate-y-1">
        <div className="bg-blue-500/20 p-3 rounded-lg text-blue-400">
          <Activity className="w-6 h-6" />
        </div>
        <div>
          <p className="text-sm text-slate-400 font-medium">Cluster Status</p>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-2xl font-bold text-slate-100">
              {cluster.leader > 0 ? `Leader Node ${cluster.leader}` : 'No Leader'}
            </span>
          </div>
          <p className="text-xs text-slate-500 mt-1">Term {cluster.term}</p>
        </div>
      </div>

      <div className="bg-slate-800 rounded-xl border border-slate-700 p-5 shadow-lg flex items-start gap-4 transition-transform hover:-translate-y-1">
        <div className={`p-3 rounded-lg ${aliveNodes > totalNodes / 2 ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}`}>
          <Server className="w-6 h-6" />
        </div>
        <div>
          <p className="text-sm text-slate-400 font-medium">Health</p>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-2xl font-bold text-slate-100">{aliveNodes} / {totalNodes}</span>
          </div>
          <p className="text-xs text-slate-500 mt-1">Nodes Alive</p>
        </div>
      </div>

      <div className="bg-slate-800 rounded-xl border border-slate-700 p-5 shadow-lg flex items-start gap-4 transition-transform hover:-translate-y-1">
        <div className="bg-purple-500/20 p-3 rounded-lg text-purple-400">
          <Database className="w-6 h-6" />
        </div>
        <div>
          <p className="text-sm text-slate-400 font-medium">Throughput</p>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-2xl font-bold text-slate-100">{leaderMetrics?.CommandsCommitted || 0}</span>
          </div>
          <p className="text-xs text-slate-500 mt-1">Total Commands</p>
        </div>
      </div>

      <div className="bg-slate-800 rounded-xl border border-slate-700 p-5 shadow-lg flex items-start gap-4 transition-transform hover:-translate-y-1">
        <div className="bg-amber-500/20 p-3 rounded-lg text-amber-400">
          <Hash className="w-6 h-6" />
        </div>
        <div>
          <p className="text-sm text-slate-400 font-medium">State Machine</p>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-2xl font-bold text-slate-100">
              Idx {leaderNode?.commitIndex || 0}
            </span>
          </div>
          <p className="text-xs text-slate-500 mt-1">Leader Commit Index</p>
        </div>
      </div>
    </div>
  );
}

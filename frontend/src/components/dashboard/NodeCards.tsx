import type { ClusterSummary, MetricsSnapshot } from '../../types';

interface Props {
  cluster: ClusterSummary;
  metrics: MetricsSnapshot[];
}

export default function NodeCards({ cluster, metrics }: Props) {
  const getStateColor = (state: string) => {
    switch (state) {
      case 'LEADER': return 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
      case 'FOLLOWER': return 'bg-blue-500/20 text-blue-400 border-blue-500/30';
      case 'CANDIDATE': return 'bg-amber-500/20 text-amber-400 border-amber-500/30';
      default: return 'bg-red-500/20 text-red-400 border-red-500/30';
    }
  };

  const getBorderColor = (state: string) => {
    switch (state) {
      case 'LEADER': return '#10b981';
      case 'FOLLOWER': return '#3b82f6';
      case 'CANDIDATE': return '#eab308';
      default: return '#ef4444';
    }
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      {cluster.nodes.map(node => {
        const metric = metrics.find(m => m.NodeID === node.id);
        
        return (
          <div 
            key={node.id} 
            className={`bg-slate-800 rounded-xl border-t-4 border-l border-r border-b border-slate-700 p-5 shadow-lg transition-all ${
              node.state === 'DEAD' ? 'opacity-60 grayscale' : 'hover:-translate-y-1 hover:shadow-xl'
            }`} 
            style={{ borderTopColor: getBorderColor(node.state) }}
          >
            <div className="flex justify-between items-start mb-5">
              <h3 className="text-xl font-bold text-slate-100 flex items-center gap-2">
                <span className="w-2 h-2 rounded-full" style={{ backgroundColor: getBorderColor(node.state) }}></span>
                Node {node.id}
              </h3>
              <span className={`px-2 py-1 text-xs font-bold rounded-md border ${getStateColor(node.state)}`}>
                {node.state}
              </span>
            </div>
            
            <div className="grid grid-cols-2 gap-3 mb-5">
              <div className="bg-slate-900/60 p-3 rounded-lg border border-slate-800">
                <p className="text-xs text-slate-400 mb-1">Term</p>
                <p className="font-mono text-lg font-semibold">{node.term}</p>
              </div>
              <div className="bg-slate-900/60 p-3 rounded-lg border border-slate-800">
                <p className="text-xs text-slate-400 mb-1">Commit Index</p>
                <p className="font-mono text-lg font-semibold">{node.commitIndex}</p>
              </div>
            </div>

            <div className="space-y-3 bg-slate-900/30 p-4 rounded-lg">
              <div className="flex justify-between text-sm items-center">
                <span className="text-slate-400 font-medium">Log Length</span>
                <span className="font-mono bg-slate-950 px-2 py-0.5 rounded">{node.logLength}</span>
              </div>
              <div className="flex justify-between text-sm items-center">
                <span className="text-slate-400 font-medium">RPCs (Sent/Recv)</span>
                <span className="font-mono bg-slate-950 px-2 py-0.5 rounded text-blue-300">
                  {metric?.RPCSent || 0} <span className="text-slate-600">/</span> {metric?.RPCReceived || 0}
                </span>
              </div>
              <div className="flex justify-between text-sm items-center">
                <span className="text-slate-400 font-medium">Heartbeats (Sent/Recv)</span>
                <span className="font-mono bg-slate-950 px-2 py-0.5 rounded text-emerald-300">
                  {metric?.HeartbeatsSent || 0} <span className="text-slate-600">/</span> {metric?.HeartbeatsReceived || 0}
                </span>
              </div>
              <div className="flex justify-between text-sm items-center">
                <span className="text-slate-400 font-medium">Cmds (Comm/App)</span>
                <span className="font-mono bg-slate-950 px-2 py-0.5 rounded text-purple-300">
                  {metric?.CommandsCommitted || 0} <span className="text-slate-600">/</span> {metric?.CommandsApplied || 0}
                </span>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

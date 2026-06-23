import { Server, Activity, ArrowUpRight, Database, ShieldCheck } from 'lucide-react';
import type { ClusterSummary, MetricsSnapshot } from '../../types';

interface Props {
  cluster: ClusterSummary;
  metrics: MetricsSnapshot[];
}

export default function ExecutiveOverview({ cluster, metrics }: Props) {
  const leaderNode = cluster.nodes.find(n => n.state === 'LEADER');
  const aliveNodes = cluster.nodes.filter(n => n.state !== 'DEAD').length;
  const healthStatus = aliveNodes > cluster.nodes.length / 2 ? 'HEALTHY' : 'DEGRADED';
  const totalRpcs = metrics.reduce((acc, curr) => acc + curr.RPCSent, 0);

  const stats = [
    {
      title: "Current Leader",
      value: leaderNode ? `Node ${leaderNode.id}` : "Election",
      subtext: `Term ${cluster.term}`,
      icon: Server,
      color: "text-primary"
    },
    {
      title: "Cluster Health",
      value: healthStatus,
      subtext: `${aliveNodes} / ${cluster.nodes.length} nodes active`,
      icon: ShieldCheck,
      color: healthStatus === 'HEALTHY' ? "text-success" : "text-danger"
    },
    {
      title: "RPC Throughput",
      value: totalRpcs.toLocaleString(),
      subtext: "Total RPCs sent",
      icon: Activity,
      color: "text-primary"
    },
    {
      title: "State Machine",
      value: leaderNode?.commitIndex.toLocaleString() || "0",
      subtext: "Commands committed",
      icon: Database,
      color: "text-primary"
    }
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      {stats.map((stat, i) => {
        const Icon = stat.icon;
        return (
          <div key={i} className="bg-panel border border-border-subtle rounded-md p-4 flex flex-col justify-between">
            <div className="flex items-start justify-between">
              <div>
                <p className="text-xs font-semibold text-text-muted uppercase tracking-wider mb-1">{stat.title}</p>
                <h3 className="text-2xl font-bold text-text-primary">{stat.value}</h3>
              </div>
              <div className={`p-2 bg-background rounded-md border border-border-subtle ${stat.color}`}>
                <Icon className="w-5 h-5" />
              </div>
            </div>
            <div className="mt-4 flex items-center gap-1.5 text-xs text-text-muted">
              {stat.title === "RPC Throughput" && <ArrowUpRight className="w-3 h-3 text-success" />}
              <span>{stat.subtext}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

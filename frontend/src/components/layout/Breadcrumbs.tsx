import { useLocation } from 'react-router-dom';
import { ChevronRight, Home } from 'lucide-react';

export default function Breadcrumbs() {
  const location = useLocation();
  const path = location.pathname.split('/').filter(p => p)[0] || 'overview';
  
  const titleMap: Record<string, string> = {
    overview: 'Executive Overview',
    topology: 'Cluster Topology',
    metrics: 'Telemetry & Metrics',
    logs: 'Log Replication Engine',
    snapshots: 'Snapshotting System',
    events: 'Event Timeline',
    nodes: 'Node Operations'
  };

  const title = titleMap[path] || 'Overview';

  return (
    <div className="flex items-center text-xs text-text-muted mb-6">
      <Home className="w-3.5 h-3.5 mr-1" />
      <span>MiniRaft</span>
      <ChevronRight className="w-3.5 h-3.5 mx-1 opacity-50" />
      <span className="text-text-primary font-medium">{title}</span>
    </div>
  );
}

import { useClusterData } from '../../hooks/useClusterData';
import ExecutiveOverview from './ExecutiveOverview';
import NodeCards from './NodeCards';
import TopologyGraph from './TopologyGraph';
import EventTimeline from './EventTimeline';
import MetricsPanel from './MetricsPanel';
import ChaosControls from '../controls/ChaosControls';
import { AlertCircle } from 'lucide-react';

export default function Dashboard() {
  const { cluster, metrics, events, error } = useClusterData();

  if (error) {
    return (
      <div className="bg-red-950/50 border border-red-900/50 text-red-200 p-6 rounded-xl flex items-center gap-4">
        <AlertCircle className="w-6 h-6 text-red-500" />
        <div>
          <h3 className="font-semibold text-red-400">Connection Error</h3>
          <p className="text-sm opacity-80">Failed to connect to MiniRaft cluster. Is the Go backend running?</p>
        </div>
      </div>
    );
  }

  if (!cluster || !metrics) {
    return (
      <div className="flex items-center justify-center h-64 text-slate-400">
        <div className="flex flex-col items-center gap-4">
          <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
          <p className="animate-pulse">Connecting to cluster...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      <ExecutiveOverview cluster={cluster} metrics={metrics} />
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 flex flex-col gap-6">
          <div className="bg-slate-800 rounded-xl border border-slate-700 shadow-xl overflow-hidden h-[450px] relative">
            <h2 className="absolute top-4 left-4 z-10 text-lg font-semibold text-slate-200 bg-slate-900/80 px-3 py-1 rounded backdrop-blur-sm">Topology</h2>
            <TopologyGraph cluster={cluster} />
          </div>
          <MetricsPanel metrics={metrics} />
        </div>
        
        <div className="flex flex-col gap-6">
          <ChaosControls />
          <div className="flex-1 bg-slate-800 rounded-xl border border-slate-700 shadow-xl overflow-hidden flex flex-col h-[400px]">
             <EventTimeline events={events} />
          </div>
        </div>
      </div>

      <div className="mt-4">
        <h2 className="text-xl font-semibold mb-4 text-slate-200 flex items-center gap-2">
          Node Details
        </h2>
        <NodeCards cluster={cluster} metrics={metrics} />
      </div>
    </div>
  );
}

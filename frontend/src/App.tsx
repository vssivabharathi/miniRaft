import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import Navbar from './components/layout/Navbar';
import Sidebar from './components/layout/Sidebar';
import Breadcrumbs from './components/layout/Breadcrumbs';
import { useClusterData } from './hooks/useClusterData';

import OverviewPage from './pages/OverviewPage';
import TopologyPage from './pages/TopologyPage';
import MetricsPage from './pages/MetricsPage';
import LogsPage from './pages/LogsPage';
import SnapshotsPage from './pages/SnapshotsPage';
import EventsPage from './pages/EventsPage';
import NodesPage from './pages/NodesPage';

import { AlertCircle, Loader2 } from 'lucide-react';

const queryClient = new QueryClient();

function AppShell() {
  // Hoist the data polling to the root shell so state is preserved across route changes
  const clusterData = useClusterData();

  if (clusterData.error) {
    return (
      <div className="flex h-screen bg-background items-center justify-center">
        <div className="bg-danger/10 border border-danger/30 text-danger p-6 rounded-md flex items-center gap-4 shadow-sm">
          <AlertCircle className="w-6 h-6" />
          <div>
            <h3 className="font-semibold">Connection Error</h3>
            <p className="text-sm opacity-80">Failed to connect to MiniRaft cluster. Ensure the Go backend is running.</p>
          </div>
        </div>
      </div>
    );
  }

  if (!clusterData.cluster || !clusterData.metrics) {
    return (
      <div className="flex h-screen bg-background items-center justify-center text-text-muted">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="w-8 h-8 animate-spin text-primary" />
          <p className="animate-pulse text-sm">Synchronizing telemetry...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-background text-text-primary font-sans overflow-hidden">
      <Sidebar />
      <div className="flex flex-col flex-1 overflow-hidden relative">
        <Navbar cluster={clusterData.cluster} />
        <main className="flex-1 overflow-y-auto p-6 custom-scrollbar bg-background">
          <Breadcrumbs />
          <div className="max-w-[1600px] mx-auto w-full pb-10">
            <Routes>
              <Route path="/" element={<Navigate to="/overview" replace />} />
              <Route path="/overview" element={<OverviewPage data={clusterData} />} />
              <Route path="/topology" element={<TopologyPage data={clusterData} />} />
              <Route path="/metrics" element={<MetricsPage data={clusterData} />} />
              <Route path="/logs" element={<LogsPage data={clusterData} />} />
              <Route path="/snapshots" element={<SnapshotsPage data={clusterData} />} />
              <Route path="/events" element={<EventsPage data={clusterData} />} />
              <Route path="/nodes" element={<NodesPage data={clusterData} />} />
              <Route path="*" element={<Navigate to="/overview" replace />} />
            </Routes>
          </div>
        </main>
      </div>
    </div>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppShell />
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import Dashboard from './components/dashboard/Dashboard';

const queryClient = new QueryClient();

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="min-h-screen bg-slate-900 text-slate-100 flex flex-col font-sans">
        <header className="bg-slate-950 border-b border-slate-800 p-4 sticky top-0 z-50">
          <div className="max-w-7xl mx-auto flex items-center justify-between">
            <h1 className="text-xl font-bold tracking-tight text-white flex items-center gap-3">
              <span className="bg-blue-600 text-white px-2 py-1 rounded text-sm tracking-widest font-black shadow-lg shadow-blue-900/50">MR</span>
              MiniRaft Console
            </h1>
            <div className="flex items-center gap-2 text-sm text-slate-400">
              <span className="relative flex h-3 w-3">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-3 w-3 bg-emerald-500"></span>
              </span>
              Cluster Active
            </div>
          </div>
        </header>
        <main className="flex-1 p-6 max-w-7xl mx-auto w-full flex flex-col gap-6">
          <Dashboard />
        </main>
      </div>
    </QueryClientProvider>
  );
}

export default App;

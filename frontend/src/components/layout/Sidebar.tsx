import { NavLink } from 'react-router-dom';
import { 
  LayoutDashboard, 
  Network, 
  LineChart, 
  FileTerminal, 
  Camera, 
  ListTree,
  Server
} from 'lucide-react';

const NAV_ITEMS = [
  { name: 'Overview', path: '/overview', icon: LayoutDashboard },
  { name: 'Topology', path: '/topology', icon: Network },
  { name: 'Metrics', path: '/metrics', icon: LineChart },
  { name: 'Log Replication', path: '/logs', icon: FileTerminal },
  { name: 'Snapshots', path: '/snapshots', icon: Camera },
  { name: 'Events', path: '/events', icon: ListTree },
  { name: 'Nodes', path: '/nodes', icon: Server },
];

export default function Sidebar() {
  return (
    <aside className="w-[240px] hidden md:flex flex-col bg-panel border-r border-border-subtle shrink-0">
      <nav className="flex-1 overflow-y-auto py-6 px-3 space-y-1">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon;
          return (
            <NavLink 
              key={item.path}
              to={item.path}
              className={({ isActive }) => `flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                isActive 
                  ? 'bg-primary/10 text-primary' 
                  : 'text-text-muted hover:bg-border-subtle hover:text-text-primary'
              }`}
            >
              {({ isActive }) => (
                <>
                  <Icon className={`w-4 h-4 ${isActive ? 'text-primary' : 'opacity-70'}`} />
                  {item.name}
                </>
              )}
            </NavLink>
          );
        })}
      </nav>
      
      <div className="p-4 border-t border-border-subtle text-xs text-text-muted text-center">
        MiniRaft Enterprise v2.0.0
      </div>
    </aside>
  );
}

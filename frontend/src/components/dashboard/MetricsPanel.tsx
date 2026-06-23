import type { MetricsSnapshot } from '../../types';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, LineChart, Line } from 'recharts';
import { BarChart3 } from 'lucide-react';

interface Props {
  metrics: MetricsSnapshot[];
}

export default function MetricsPanel({ metrics }: Props) {
  const rpcData = metrics.map(m => ({
    name: `Node ${m.NodeID}`,
    Sent: m.RPCSent,
    Received: m.RPCReceived,
  }));

  const heartbeatData = metrics.map(m => ({
    name: `Node ${m.NodeID}`,
    Sent: m.HeartbeatsSent,
    Received: m.HeartbeatsReceived,
  }));

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-slate-900 border border-slate-700 p-3 rounded-lg shadow-xl">
          <p className="font-bold text-slate-200 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={`item-${index}`} style={{ color: entry.color }} className="text-sm">
              {entry.name}: <span className="font-mono">{entry.value}</span>
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="bg-slate-800 rounded-xl border border-slate-700 p-5 shadow-xl">
      <h2 className="text-lg font-semibold text-slate-200 mb-6 flex items-center gap-2">
        <BarChart3 className="w-5 h-5 text-indigo-400" />
        Cluster Telemetry
      </h2>
      
      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        <div className="h-64">
          <h3 className="text-sm font-medium text-slate-400 mb-4 text-center">RPC Throughput</h3>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={rpcData} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" vertical={false} />
              <XAxis dataKey="name" stroke="#94a3b8" fontSize={12} tickLine={false} axisLine={false} />
              <YAxis stroke="#94a3b8" fontSize={12} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} cursor={{ fill: '#334155', opacity: 0.4 }} />
              <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
              <Bar dataKey="Sent" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
              <Bar dataKey="Received" fill="#0ea5e9" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="h-64">
          <h3 className="text-sm font-medium text-slate-400 mb-4 text-center">Heartbeat Distribution</h3>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={heartbeatData} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" vertical={false} />
              <XAxis dataKey="name" stroke="#94a3b8" fontSize={12} tickLine={false} axisLine={false} />
              <YAxis stroke="#94a3b8" fontSize={12} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
              <Line type="monotone" dataKey="Sent" stroke="#10b981" strokeWidth={3} dot={{ r: 4, strokeWidth: 2 }} activeDot={{ r: 6 }} />
              <Line type="monotone" dataKey="Received" stroke="#f59e0b" strokeWidth={3} dot={{ r: 4, strokeWidth: 2 }} activeDot={{ r: 6 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}

import React, { useMemo } from 'react';
import type { MetricsSnapshot } from '../../types';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, LineChart, Line } from 'recharts';
import { LineChart as LineChartIcon } from 'lucide-react';

interface Props {
  metrics: MetricsSnapshot[];
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-panel border border-border-subtle p-3 rounded-md shadow-lg">
        <p className="font-semibold text-text-primary text-sm mb-2">{label}</p>
        {payload.map((entry: any, index: number) => (
          <p key={`item-${index}`} style={{ color: entry.color }} className="text-xs">
            {entry.name}: <span className="font-mono">{entry.value}</span>
          </p>
        ))}
      </div>
    );
  }
  return null;
};

const MetricsPanelComponent = ({ metrics }: Props) => {
  const rpcData = useMemo(() => metrics.map(m => ({
    name: `Node ${m.NodeID}`,
    Sent: m.RPCSent,
    Received: m.RPCReceived,
  })), [metrics]);

  const heartbeatData = useMemo(() => metrics.map(m => ({
    name: `Node ${m.NodeID}`,
    Sent: m.HeartbeatsSent,
    Received: m.HeartbeatsReceived,
  })), [metrics]);

  return (
    <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm">
      <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider mb-6 flex items-center gap-2">
        <LineChartIcon className="w-4 h-4 text-primary" />
        Cluster Telemetry
      </h2>
      
      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        <div className="h-64">
          <h3 className="text-xs font-medium text-text-muted mb-4">RPC Throughput</h3>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={rpcData} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-color)" vertical={false} />
              <XAxis dataKey="name" stroke="var(--text-secondary)" fontSize={11} tickLine={false} axisLine={false} />
              <YAxis stroke="var(--text-secondary)" fontSize={11} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} cursor={{ fill: 'var(--border-color)', opacity: 0.4 }} />
              <Legend wrapperStyle={{ fontSize: '11px', paddingTop: '10px', color: 'var(--text-secondary)' }} />
              <Bar dataKey="Sent" fill="var(--primary)" radius={[2, 2, 0, 0]} />
              <Bar dataKey="Received" fill="var(--warning)" radius={[2, 2, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="h-64">
          <h3 className="text-xs font-medium text-text-muted mb-4">Heartbeat Distribution</h3>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={heartbeatData} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-color)" vertical={false} />
              <XAxis dataKey="name" stroke="var(--text-secondary)" fontSize={11} tickLine={false} axisLine={false} />
              <YAxis stroke="var(--text-secondary)" fontSize={11} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: '11px', paddingTop: '10px', color: 'var(--text-secondary)' }} />
              <Line type="monotone" dataKey="Sent" stroke="var(--success)" strokeWidth={2} dot={{ r: 3, strokeWidth: 1 }} activeDot={{ r: 5 }} />
              <Line type="monotone" dataKey="Received" stroke="var(--danger)" strokeWidth={2} dot={{ r: 3, strokeWidth: 1 }} activeDot={{ r: 5 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};

export default React.memo(MetricsPanelComponent);

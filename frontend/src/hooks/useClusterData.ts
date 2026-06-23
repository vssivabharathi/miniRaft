import { useQuery } from '@tanstack/react-query';
import type { 
  ClusterSummary, 
  MetricsSnapshot, 
  ClusterEvent, 
  RpcType, 
  NodeLogSummary, 
  NodeSnapshotSummary, 
  NodeStateMachineSummary 
} from '../types';
import { useState, useEffect, useRef } from 'react';

export function useClusterData() {
  const [events, setEvents] = useState<ClusterEvent[]>([]);
  const prevCluster = useRef<ClusterSummary | null>(null);
  const prevMetrics = useRef<MetricsSnapshot[] | null>(null);
  
  const [activeRpc, setActiveRpc] = useState<RpcType>('Heartbeat');
  const [snapshotStats, setSnapshotStats] = useState({ saved: 0, count: 0 });

  const { data: cluster, error: clusterError } = useQuery<ClusterSummary>({
    queryKey: ['cluster'],
    queryFn: async () => {
      const res = await fetch('/cluster');
      if (!res.ok) throw new Error('Failed to fetch cluster');
      return res.json();
    },
    refetchInterval: 1000,
  });

  const { data: metrics, error: metricsError } = useQuery<MetricsSnapshot[]>({
    queryKey: ['metrics'],
    queryFn: async () => {
      const res = await fetch('/metrics');
      if (!res.ok) throw new Error('Failed to fetch metrics');
      const data = await res.json();
      return data || [];
    },
    refetchInterval: 1000,
  });

  const { data: logsData, error: logsError, isLoading: logsLoading } = useQuery<{ nodes: NodeLogSummary[] }>({
    queryKey: ['logs'],
    queryFn: async () => {
      const res = await fetch('/api/logs');
      if (!res.ok) throw new Error('Failed to fetch logs');
      return res.json();
    },
    refetchInterval: 1000,
  });

  const { data: snapshotsData, error: snapshotsError, isLoading: snapshotsLoading } = useQuery<{ nodes: NodeSnapshotSummary[] }>({
    queryKey: ['snapshots'],
    queryFn: async () => {
      const res = await fetch('/api/snapshots');
      if (!res.ok) throw new Error('Failed to fetch snapshots');
      return res.json();
    },
    refetchInterval: 1000,
  });

  const { data: stateMachineData, error: stateMachineError, isLoading: stateMachineLoading } = useQuery<{ nodes: NodeStateMachineSummary[] }>({
    queryKey: ['stateMachine'],
    queryFn: async () => {
      const res = await fetch('/api/state-machine');
      if (!res.ok) throw new Error('Failed to fetch state machine');
      return res.json();
    },
    refetchInterval: 1000,
  });

  useEffect(() => {
    if (!cluster || !metrics) return;
    
    const newEvents: ClusterEvent[] = [];
    let dominantRpc: RpcType = 'Heartbeat';
    
    if (prevCluster.current) {
      if (prevCluster.current.leader !== cluster.leader) {
        if (cluster.leader > 0) {
          newEvents.push({
            id: crypto.randomUUID(),
            timestamp: new Date(),
            message: `Leader elected: Node ${cluster.leader} (Term ${cluster.term})`,
            type: 'success'
          });
        } else {
          newEvents.push({
            id: crypto.randomUUID(),
            timestamp: new Date(),
            message: `Cluster lost leader`,
            type: 'warning'
          });
        }
      }

      cluster.nodes.forEach(node => {
        const prevNode = prevCluster.current?.nodes.find(n => n.id === node.id);
        if (prevNode && prevNode.state !== node.state) {
          let type: ClusterEvent['type'] = 'info';
          if (node.state === 'DEAD') type = 'error';
          if (node.state === 'LEADER') type = 'success';
          if (node.state === 'CANDIDATE') type = 'warning';
          
          if (prevNode.state === 'DEAD' && node.state !== 'DEAD') {
             newEvents.push({
               id: crypto.randomUUID(),
               timestamp: new Date(),
               message: `Node ${node.id} restarted`,
               type: 'success'
             });
          }
          
          newEvents.push({
            id: crypto.randomUUID(),
            timestamp: new Date(),
            message: `Node ${node.id} changed state to ${node.state}`,
            type
          });
        }
      });
    }

    if (prevMetrics.current && cluster.leader > 0) {
      const leaderCurrent = metrics.find(m => m.NodeID === cluster.leader);
      const leaderPrev = prevMetrics.current.find(m => m.NodeID === cluster.leader);

      if (leaderCurrent && leaderPrev) {
        // Detect RPC activity
        const commitDiff = leaderCurrent.CommandsCommitted - leaderPrev.CommandsCommitted;
        const logLengthDiff = leaderCurrent.LogLength - leaderPrev.LogLength;

        if (commitDiff > 0) {
          dominantRpc = 'AppendEntries';
          newEvents.push({
            id: crypto.randomUUID(),
            timestamp: new Date(),
            message: `Replicated ${commitDiff} new command(s) (Commit Index ${leaderCurrent.CommitIndex})`,
            type: 'info'
          });
        }

        // Detect snapshot / log compaction
        // If log length drops by a significant margin but we haven't restarted
        if (logLengthDiff < -5) {
          dominantRpc = 'InstallSnapshot';
          const saved = Math.abs(logLengthDiff);
          setSnapshotStats(s => ({ saved: s.saved + saved, count: s.count + 1 }));
          newEvents.push({
            id: crypto.randomUUID(),
            timestamp: new Date(),
            message: `📸 Snapshot created! Log compacted by ${saved} entries.`,
            type: 'success'
          });
        }
      }
    }

    setActiveRpc(dominantRpc);

    if (newEvents.length > 0) {
      setEvents(prev => [...prev, ...newEvents].slice(-100));
    }

    prevCluster.current = cluster;
    prevMetrics.current = metrics;
  }, [cluster, metrics]);

  const leaderMetrics = metrics?.find(m => m.NodeID === cluster?.leader);
  const compactedIndex = leaderMetrics ? Math.max(0, leaderMetrics.CommitIndex - leaderMetrics.LogLength) : 0;

  return { 
    cluster, 
    metrics, 
    events, 
    error: clusterError || metricsError || logsError || snapshotsError || stateMachineError,
    activeRpc,
    compactedIndex,
    snapshotStats,
    logs: logsData?.nodes ?? [],
    snapshots: snapshotsData?.nodes ?? [],
    stateMachine: stateMachineData?.nodes ?? [],
    isLoading: {
      logs: logsLoading,
      snapshots: snapshotsLoading,
      stateMachine: stateMachineLoading
    }
  };
}

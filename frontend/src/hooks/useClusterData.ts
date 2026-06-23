import { useQuery } from '@tanstack/react-query';
import type { ClusterSummary, MetricsSnapshot, ClusterEvent } from '../types';
import { useState, useEffect, useRef } from 'react';

export function useClusterData() {
  const [events, setEvents] = useState<ClusterEvent[]>([]);
  const prevCluster = useRef<ClusterSummary | null>(null);

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

  useEffect(() => {
    if (!cluster) return;
    
    const newEvents: ClusterEvent[] = [];
    
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
          
          newEvents.push({
            id: crypto.randomUUID(),
            timestamp: new Date(),
            message: `Node ${node.id} changed state to ${node.state}`,
            type
          });
        }
      });
    }

    if (newEvents.length > 0) {
      setEvents(prev => [...prev, ...newEvents].slice(-100)); // Keep last 100
    }

    prevCluster.current = cluster;
  }, [cluster]);

  return { cluster, metrics, events, error: clusterError || metricsError };
}

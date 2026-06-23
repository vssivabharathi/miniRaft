import React, { useEffect, useRef, useState } from 'react';
import { ReactFlow, useNodesState, useEdgesState, MarkerType, Background, Position, Handle } from '@xyflow/react';
import { useQuery } from '@tanstack/react-query';
import '@xyflow/react/dist/style.css';
import type { ClusterSummary, RpcType, MetricsSnapshot, NodeFullSummary } from '../../types';
import type { Node, Edge } from '@xyflow/react';
import { X, Server, Activity, Database, HeartPulse, FileTerminal } from 'lucide-react';

const CustomNode = ({ data }: any) => {
  return (
    <div className={`px-4 py-3 rounded-md border text-center min-w-[140px] shadow-sm transition-colors relative bg-panel cursor-pointer hover:shadow-md hover:-translate-y-0.5 ${
      data.state === 'LEADER' ? 'border-success ring-1 ring-success/50' :
      data.state === 'FOLLOWER' ? 'border-primary' :
      data.state === 'CANDIDATE' ? 'border-warning' :
      'border-border-subtle opacity-50 grayscale'
    }`}>
      <Handle type="target" position={Position.Top} className="opacity-0" />
      <div className="font-semibold text-text-primary text-base">Node {data.id}</div>
      <div className={`text-[10px] font-bold mt-0.5 tracking-wider uppercase ${
        data.state === 'LEADER' ? 'text-success' :
        data.state === 'FOLLOWER' ? 'text-primary' :
        data.state === 'CANDIDATE' ? 'text-warning' :
        'text-text-muted'
      }`}>{data.state}</div>
      <Handle type="source" position={Position.Bottom} className="opacity-0" />
    </div>
  );
};

const nodeTypes = {
  custom: CustomNode,
};

interface Props {
  cluster: ClusterSummary;
  metrics: MetricsSnapshot[];
  activeRpc?: RpcType;
}

const TopologyGraphComponent = ({ cluster, metrics, activeRpc = 'Heartbeat' }: Props) => {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  
  const [selectedNodeId, setSelectedNodeId] = useState<number | null>(null);
  const [hoveredEdge, setHoveredEdge] = useState<{ label: string, rpc: string, x: number, y: number } | null>(null);

  const prevStateHash = useRef('');

  useEffect(() => {
    if (!cluster) return;

    const leaderNode = cluster.nodes.find(n => n.state === 'LEADER');
    const currentStateHash = cluster.nodes.map(n => `${n.id}:${n.state}`).join(',') + `|${activeRpc}`;
    
    if (prevStateHash.current === currentStateHash) {
      return; 
    }
    prevStateHash.current = currentStateHash;

    const newNodes = cluster.nodes.map((node) => {
      let x = 0; let y = 0;
      if (node.state === 'LEADER') {
        x = 250; y = 50;
      } else {
        const others = cluster.nodes.filter(n => n.state !== 'LEADER');
        const index = others.findIndex(n => n.id === node.id);
        const total = others.length;
        const spacing = 200;
        const startX = 250 - ((total - 1) * spacing) / 2;
        x = startX + (index * spacing);
        y = 200;
        if (node.state === 'DEAD') y = 280;
      }

      return {
        id: `node-${node.id}`,
        type: 'custom',
        position: { x, y },
        data: { id: node.id, state: node.state },
        draggable: false, 
      };
    });

    const newEdges: any[] = [];
    if (leaderNode) {
      let edgeColor = 'var(--success)';
      let edgeLabel = 'Heartbeat';
      let strokeWidth = 1.5;
      
      if (activeRpc === 'AppendEntries') {
        edgeColor = 'var(--primary)';
        edgeLabel = 'AppendEntries';
        strokeWidth = 2;
      } else if (activeRpc === 'InstallSnapshot') {
        edgeColor = 'var(--warning)'; 
        edgeLabel = 'InstallSnapshot';
        strokeWidth = 3;
      }

      cluster.nodes.forEach(node => {
        if (node.id !== leaderNode.id && node.state !== 'DEAD') {
          newEdges.push({
            id: `edge-${leaderNode.id}-${node.id}`,
            source: `node-${leaderNode.id}`,
            target: `node-${node.id}`,
            animated: activeRpc !== 'Heartbeat', 
            label: edgeLabel,
            style: { stroke: edgeColor, strokeWidth, cursor: 'pointer' },
            labelStyle: { fill: 'var(--text-muted)', fontSize: 10, fontWeight: 500, pointerEvents: 'none' },
            labelBgStyle: { fill: 'var(--panel-bg)', fillOpacity: 0.9, pointerEvents: 'none' },
            labelBgPadding: [6, 4],
            labelBgBorderRadius: 2,
            markerEnd: { type: MarkerType.ArrowClosed, color: edgeColor },
            data: { rpc: edgeLabel }
          });
        }
      });
    }

    setNodes(newNodes);
    setEdges(newEdges);
  }, [cluster, setNodes, setEdges, activeRpc]);

  const onNodeClick = (_: any, node: Node) => {
    setSelectedNodeId(node.data.id as number);
  };

  const onEdgeMouseEnter = (e: any, edge: Edge) => {
    setHoveredEdge({
      label: `${edge.source.replace('node-', 'Node ')} -> ${edge.target.replace('node-', 'Node ')}`,
      rpc: edge.data?.rpc as string || 'Heartbeat',
      x: e.clientX,
      y: e.clientY
    });
  };

  const onEdgeMouseLeave = () => setHoveredEdge(null);

  const selectedNode = selectedNodeId ? cluster?.nodes.find(n => n.id === selectedNodeId) : null;
  const selectedMetrics = selectedNodeId ? metrics?.find(m => m.NodeID === selectedNodeId) : null;

  const { data: fullNodeDetails } = useQuery<NodeFullSummary>({
    queryKey: ['nodeDetails', selectedNodeId],
    queryFn: async () => {
      if (!selectedNodeId) return null;
      const res = await fetch(`/api/node/${selectedNodeId}`);
      if (!res.ok) throw new Error('Failed to fetch node details');
      return res.json();
    },
    enabled: !!selectedNodeId,
    refetchInterval: 1000,
  });

  return (
    <div className="w-full h-full bg-background rounded-md relative border border-border-subtle overflow-hidden flex">
      <div className="flex-1 relative">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick}
          onEdgeMouseEnter={onEdgeMouseEnter}
          onEdgeMouseLeave={onEdgeMouseLeave}
          nodeTypes={nodeTypes}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          proOptions={{ hideAttribution: true }}
          panOnDrag={false}
          zoomOnScroll={false}
          nodesDraggable={false}
        >
          <Background color="var(--border-color)" gap={20} size={1} />
        </ReactFlow>

        {/* Edge Tooltip */}
        {hoveredEdge && (
          <div 
            className="fixed z-50 bg-panel border border-border-subtle p-3 rounded shadow-lg pointer-events-none transform -translate-x-1/2 -translate-y-full"
            style={{ left: hoveredEdge.x, top: hoveredEdge.y - 15 }}
          >
            <div className="text-xs font-semibold text-text-primary mb-1">{hoveredEdge.label}</div>
            <div className="text-[10px] text-text-muted uppercase tracking-wider mb-2">Traffic Type</div>
            <div className={`text-xs font-mono font-medium ${
              hoveredEdge.rpc === 'AppendEntries' ? 'text-primary' :
              hoveredEdge.rpc === 'InstallSnapshot' ? 'text-warning' : 'text-success'
            }`}>{hoveredEdge.rpc}</div>
          </div>
        )}
      </div>

      {/* Node Details Drawer */}
      {selectedNode && (
        <div className="w-[300px] border-l border-border-subtle bg-panel shadow-2xl flex flex-col z-20 shrink-0">
          <div className="p-4 border-b border-border-subtle flex items-center justify-between bg-background/50">
            <h3 className="font-semibold flex items-center gap-2">
              <Server className="w-4 h-4 text-primary" />
              Node {selectedNode.id}
            </h3>
            <button 
              onClick={() => setSelectedNodeId(null)}
              className="p-1 hover:bg-border-subtle rounded transition-colors text-text-muted hover:text-text-primary"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
          
          <div className="p-4 flex-1 overflow-y-auto space-y-4 custom-scrollbar">
            <div>
              <div className="text-xs text-text-muted uppercase tracking-wider mb-1">State</div>
              <div className={`font-mono text-sm font-bold ${
                selectedNode.state === 'LEADER' ? 'text-success' :
                selectedNode.state === 'FOLLOWER' ? 'text-primary' :
                selectedNode.state === 'DEAD' ? 'text-danger' : 'text-warning'
              }`}>{selectedNode.state}</div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <div className="text-xs text-text-muted uppercase tracking-wider mb-1 flex items-center gap-1">
                  <Database className="w-3 h-3" /> Term
                </div>
                <div className="font-mono text-sm">{selectedNode.term}</div>
              </div>
              <div>
                <div className="text-xs text-text-muted uppercase tracking-wider mb-1 flex items-center gap-1">
                  <Database className="w-3 h-3" /> Log Len
                </div>
                <div className="font-mono text-sm">{selectedNode.logLength}</div>
              </div>
            </div>

            <div>
              <div className="text-xs text-text-muted uppercase tracking-wider mb-1 flex items-center gap-1">
                <Database className="w-3 h-3" /> Commit & Apply
              </div>
              <div className="flex justify-between text-sm font-mono bg-background p-2 rounded border border-border-subtle">
                <span className="text-success" title="Commit Index">Idx: {fullNodeDetails ? fullNodeDetails.commitIndex : selectedNode.commitIndex}</span>
                <span className="text-primary" title="Last Applied">App: {fullNodeDetails?.lastApplied ?? '?'}</span>
              </div>
            </div>

            <hr className="border-border-subtle my-2" />

            <div className="space-y-3">
              <div>
                <div className="text-xs text-text-muted uppercase tracking-wider mb-1 flex items-center gap-1">
                  <Activity className="w-3 h-3" /> RPC Traffic
                </div>
                <div className="flex justify-between text-sm font-mono bg-background p-2 rounded border border-border-subtle">
                  <span className="text-primary">Sent: {fullNodeDetails?.rpcSent ?? selectedMetrics?.RPCSent ?? 0}</span>
                  <span className="text-warning">Recv: {fullNodeDetails?.rpcReceived ?? selectedMetrics?.RPCReceived ?? 0}</span>
                </div>
              </div>

              <div>
                <div className="text-xs text-text-muted uppercase tracking-wider mb-1 flex items-center gap-1">
                  <HeartPulse className="w-3 h-3" /> Heartbeats
                </div>
                <div className="flex justify-between text-sm font-mono bg-background p-2 rounded border border-border-subtle">
                  <span className="text-success">Sent: {fullNodeDetails?.heartbeatsSent ?? selectedMetrics?.HeartbeatsSent ?? 0}</span>
                  <span className="text-danger">Recv: {fullNodeDetails?.heartbeatsReceived ?? selectedMetrics?.HeartbeatsReceived ?? 0}</span>
                </div>
              </div>
            </div>

            {fullNodeDetails && (
              <>
                <hr className="border-border-subtle my-2" />
                <div>
                  <div className="text-xs text-text-muted uppercase tracking-wider mb-1 flex items-center gap-1">
                    <FileTerminal className="w-3 h-3" /> Log Entries ({fullNodeDetails.entries?.length || 0})
                  </div>
                  <div className="bg-background border border-border-subtle rounded max-h-32 overflow-y-auto custom-scrollbar p-1">
                    {(!fullNodeDetails.entries || fullNodeDetails.entries.length === 0) ? (
                      <div className="text-[10px] text-text-muted text-center p-2">Log is empty</div>
                    ) : (
                      fullNodeDetails.entries.slice().reverse().map(e => (
                        <div key={e.index} className="flex justify-between items-center text-[10px] font-mono px-2 py-1 border-b border-border-subtle last:border-0">
                          <span className="text-text-muted">[{e.index}]</span>
                          <span className="text-primary truncate mx-2 max-w-[100px]">{e.command}</span>
                          <span className={e.committed ? 'text-success' : 'text-warning'}>{e.committed ? 'C' : 'R'}</span>
                        </div>
                      ))
                    )}
                  </div>
                </div>

                <div>
                  <div className="text-xs text-text-muted uppercase tracking-wider mb-1 flex items-center gap-1">
                    <Database className="w-3 h-3" /> Local KV State
                  </div>
                  <div className="bg-background border border-border-subtle rounded max-h-32 overflow-y-auto custom-scrollbar p-2 text-xs font-mono">
                    {(!fullNodeDetails.kv || Object.keys(fullNodeDetails.kv).length === 0) ? (
                      <div className="text-[10px] text-text-muted text-center">KV Store empty</div>
                    ) : (
                      Object.entries(fullNodeDetails.kv).map(([k, v]) => (
                        <div key={k} className="flex justify-between py-0.5">
                          <span className="text-text-primary">{k}</span>
                          <span className="text-success">{v}</span>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default React.memo(TopologyGraphComponent);

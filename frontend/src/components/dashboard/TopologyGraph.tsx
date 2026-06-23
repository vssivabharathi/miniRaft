import { useEffect } from 'react';
import { ReactFlow, useNodesState, useEdgesState, MarkerType, Background, Position, Handle } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import type { ClusterSummary } from '../../types';
import type { Node, Edge } from '@xyflow/react';

// Custom Node Component to render beautiful status cards in the graph
const CustomNode = ({ data }: any) => {
  return (
    <div className={`px-6 py-4 rounded-xl border-2 shadow-xl text-center min-w-[140px] transition-all duration-300 ${
      data.state === 'LEADER' ? 'bg-emerald-900/50 border-emerald-500 shadow-emerald-900/50 ring-4 ring-emerald-500/20 ring-offset-2 ring-offset-slate-900' :
      data.state === 'FOLLOWER' ? 'bg-blue-900/50 border-blue-500 shadow-blue-900/50' :
      data.state === 'CANDIDATE' ? 'bg-amber-900/50 border-amber-500 shadow-amber-900/50' :
      'bg-red-950/50 border-red-900/50 shadow-none opacity-50 grayscale'
    }`}>
      <Handle type="target" position={Position.Top} className="!bg-slate-500 !w-3 !h-3 !border-2 !border-slate-800 opacity-0" />
      <div className="font-bold text-slate-100 text-lg">Node {data.id}</div>
      <div className={`text-xs font-black mt-1 tracking-widest uppercase ${
        data.state === 'LEADER' ? 'text-emerald-400' :
        data.state === 'FOLLOWER' ? 'text-blue-400' :
        data.state === 'CANDIDATE' ? 'text-amber-400' :
        'text-red-500'
      }`}>{data.state}</div>
      <Handle type="source" position={Position.Bottom} className="!bg-slate-500 !w-3 !h-3 !border-2 !border-slate-800 opacity-0" />
    </div>
  );
};

const nodeTypes = {
  custom: CustomNode,
};

interface Props {
  cluster: ClusterSummary;
}

export default function TopologyGraph({ cluster }: Props) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);

  useEffect(() => {
    if (!cluster) return;

    const leaderNode = cluster.nodes.find(n => n.state === 'LEADER');
    
    // Auto-layout logic
    const newNodes = cluster.nodes.map((node) => {
      let x = 0;
      let y = 0;
      
      if (node.state === 'LEADER') {
        x = 250;
        y = 50;
      } else {
        const others = cluster.nodes.filter(n => n.state !== 'LEADER');
        const index = others.findIndex(n => n.id === node.id);
        const total = others.length;
        const spacing = 250;
        const startX = 250 - ((total - 1) * spacing) / 2;
        
        x = startX + (index * spacing);
        y = 250;
        
        if (node.state === 'DEAD') {
            y = 350; // Push dead nodes down slightly
        }
      }

      return {
        id: `node-${node.id}`,
        type: 'custom',
        position: { x, y },
        data: { id: node.id, state: node.state },
        draggable: true,
      };
    });

    const newEdges: any[] = [];
    if (leaderNode) {
      cluster.nodes.forEach(node => {
        if (node.id !== leaderNode.id && node.state !== 'DEAD') {
          newEdges.push({
            id: `edge-${leaderNode.id}-${node.id}`,
            source: `node-${leaderNode.id}`,
            target: `node-${node.id}`,
            animated: true, // Animates the edge to show RPC flow
            label: 'AppendEntries',
            style: { stroke: '#3b82f6', strokeWidth: 2 },
            labelStyle: { fill: '#94a3b8', fontSize: 11, fontWeight: 600, fontFamily: 'monospace' },
            labelBgStyle: { fill: '#0f172a', fillOpacity: 0.9 },
            labelBgPadding: [8, 4],
            labelBgBorderRadius: 4,
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: '#3b82f6',
            },
          });
        }
      });
    }

    setNodes(newNodes);
    setEdges(newEdges);
  }, [cluster, setNodes, setEdges]);

  return (
    <div className="w-full h-full bg-slate-900 relative">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.5, minZoom: 0.5, maxZoom: 1.2 }}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#334155" gap={16} size={1} />
      </ReactFlow>
    </div>
  );
}

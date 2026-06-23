export type NodeState = 'LEADER' | 'FOLLOWER' | 'CANDIDATE' | 'DEAD';

export interface NodeSummary {
  id: number;
  state: NodeState;
  term: number;
  commitIndex: number;
  logLength: number;
}

export interface ClusterSummary {
  leader: number;
  term: number;
  nodes: NodeSummary[];
}

export interface MetricsSnapshot {
  NodeID: number;
  State: number;
  CurrentTerm: number;
  ElectionsWon: number;
  ElectionsLost: number;
  RPCSent: number;
  RPCReceived: number;
  CommandsCommitted: number;
  CommandsApplied: number;
  HeartbeatsSent: number;
  HeartbeatsReceived: number;
  CommitIndex: number;
  LastApplied: number;
  LogLength: number;
}

export interface ClusterEvent {
  id: string;
  timestamp: Date;
  message: string;
  type: 'info' | 'warning' | 'error' | 'success';
}

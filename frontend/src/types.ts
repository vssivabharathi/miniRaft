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

export interface LogEntrySummary {
  index: number;
  term: number;
  command: string;
  committed: boolean;
}

export interface NodeLogSummary {
  node_id: number;
  state: string;
  entries: LogEntrySummary[];
}

export interface NodeSnapshotSummary {
  node_id: number;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  physicalLogLength: number;
  compactedEntries: number;
}

export interface NodeStateMachineSummary {
  node_id: number;
  kv: Record<string, string>;
}

export interface NodeFullSummary {
  id: number;
  state: string;
  term: number;
  commitIndex: number;
  lastApplied: number;
  logLength: number;
  snapshotLastIncludedIndex: number;
  snapshotLastIncludedTerm: number;
  rpcSent: number;
  rpcReceived: number;
  heartbeatsSent: number;
  heartbeatsReceived: number;
  entries: LogEntrySummary[];
  kv: Record<string, string>;
}

export type RpcType = 'Heartbeat' | 'AppendEntries' | 'InstallSnapshot' | 'None';

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
